import json
import logging
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# constants
NOTIFICATION_TYPE_STABILIZED = "Stabilized"
NOTIFICATION_TYPE_OUT_OF_RANGE = "OutOfRange"
VALID_NOTIFICATION_TYPES = {NOTIFICATION_TYPE_STABILIZED, NOTIFICATION_TYPE_OUT_OF_RANGE}


def _resolve_token(host: str) -> Optional[str]:
    """resolves the notification token from env or credentials file."""
    # check explicit environment variable first
    if token := os.environ.get("NOTIFICATIONS_TOKEN"):
        if token.lower() in ("none", "false", "0", ""):
            return None
        return token.strip()

    # only attach credentials token for the remote demo server
    if "cec.dlandau.nl" not in host:
        return None

    # check credentials file in ./auth/token
    token_path = os.environ.get("NOTIFICATIONS_TOKEN_PATH", "./auth/token")
    if os.path.isfile(token_path):
        try:
            with open(token_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if content:
                    return content
        except Exception as e:
            logger.warning("Could not read token file at %s: %s", token_path, e)

    return None


class NotificationClient:
    """client for interacting with the notifications service (interface 3).

    handles dispatching notifications for:
      - 'Stabilized': when temperature first reaches desired range in stabilization phase.
      - 'OutOfRange': when temperature deviates outside allowed range in carrying-out phase.

    dispatches requests via a background thread pool by default to prevent blocking
    the kafka consumer loop, keeping latency well under the required 10-second threshold.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        max_workers: int = 5,
        timeout: float = 5.0,
        on_latency: Optional[Callable[[float, dict], None]] = None,
    ):
        # base host can be set via env var NOTIFICATIONS_HOST
        # local default: http://localhost:3000
        # demo host: https://notifications.cec.dlandau.nl
        self.host = (host or os.environ.get("NOTIFICATIONS_HOST", "http://localhost:3000")).rstrip("/")
        self.token = token if token is not None else _resolve_token(self.host)
        self.timeout = timeout
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="notifier-worker")
        self.on_latency = on_latency

        # Latency metrics & SLA tracking (< 10.0s requirement)
        self._stats_lock = threading.Lock()
        self.total_sent = 0
        self.total_latency = 0.0
        self.min_latency = float("inf")
        self.max_latency = 0.0
        self.sla_violations = 0

    def _build_url(self) -> str:
        """constructs the notification endpoint url, attaching the token query parameter if present."""
        base_url = f"{self.host}/api/notify"
        if self.token:
            query = urllib.parse.urlencode({"token": self.token})
            return f"{base_url}?{query}"
        return base_url

    def _send_request_sync(self, payload: dict) -> Optional[float]:
        """synchronously executes the http post request to the notifications endpoint."""
        url = self._build_url()
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            data=data,
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "Accept": "*/*",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                status = resp.status
                body = resp.read().decode("utf-8").strip()
                if status == 200:
                    latency = None
                    try:
                        latency = float(body)
                    except ValueError:
                        pass

                    if latency is not None:
                        with self._stats_lock:
                            self.total_sent += 1
                            self.total_latency += latency
                            self.min_latency = min(self.min_latency, latency)
                            self.max_latency = max(self.max_latency, latency)
                            if latency >= 10.0:
                                self.sla_violations += 1

                        logger.info(
                            "Notification sent successfully [%s] for experiment=%s, measurement=%s. Service latency: %s",
                            payload.get("notification_type"),
                            payload.get("experiment_id"),
                            payload.get("measurement_id"),
                            body,
                        )

                        if self.on_latency:
                            try:
                                self.on_latency(latency, payload)
                            except Exception as cb_err:
                                logger.warning("Error in on_latency callback: %s", cb_err)

                        return latency
                    else:
                        logger.info(
                            "Notification sent successfully [%s] for experiment=%s, measurement=%s. Body: %s",
                            payload.get("notification_type"),
                            payload.get("experiment_id"),
                            payload.get("measurement_id"),
                            body,
                        )
                        return None
                else:
                    logger.error(
                        "Notification returned unexpected status %d: %s",
                        status,
                        body,
                    )
                    return None
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8", errors="replace")
            logger.error(
                "HTTP error sending notification to %s [%d]: %s",
                url,
                e.code,
                error_body,
            )
            return None
        except urllib.error.URLError as e:
            logger.error("Network error sending notification to %s: %s", url, e.reason)
            return None
        except Exception as e:
            logger.error("Unexpected error sending notification to %s: %s", url, e)
            return None

    def send_notification(
        self,
        notification_type: str,
        researcher: Optional[str],
        experiment_id: str,
        measurement_id: str,
        cipher_data: str,
        async_send: bool = True,
    ):
        """validates payload and sends a notification.

        args:
            notification_type: 'Stabilized' or 'OutOfRange'.
            researcher: email address of the researcher.
            experiment_id: unique uuid of the experiment.
            measurement_id: id of the measurement triggering the event.
            cipher_data: the measurement_hash from the sensor measurement event.
            async_send: if True (default), runs in background thread pool.
        """
        if notification_type not in VALID_NOTIFICATION_TYPES:
            raise ValueError(
                f"Invalid notification_type '{notification_type}'. Expected one of: {VALID_NOTIFICATION_TYPES}"
            )

        if not researcher:
            logger.warning(
                "Attempted to send notification without a researcher email for experiment %s",
                experiment_id,
            )

        payload = {
            "notification_type": notification_type,
            "researcher": researcher or "",
            "experiment_id": experiment_id,
            "measurement_id": measurement_id,
            "cipher_data": cipher_data,
        }

        if async_send:
            return self.executor.submit(self._send_request_sync, payload)
        return self._send_request_sync(payload)

    def notify_stabilized(
        self,
        researcher: Optional[str],
        experiment_id: str,
        measurement_id: str,
        cipher_data: str,
        async_send: bool = True,
    ):
        """convenience method to notify when an experiment has reached its target temperature range."""
        return self.send_notification(
            notification_type=NOTIFICATION_TYPE_STABILIZED,
            researcher=researcher,
            experiment_id=experiment_id,
            measurement_id=measurement_id,
            cipher_data=cipher_data,
            async_send=async_send,
        )

    def notify_out_of_range(
        self,
        researcher: Optional[str],
        experiment_id: str,
        measurement_id: str,
        cipher_data: str,
        async_send: bool = True,
    ):
        """convenience method to notify when an experiment falls out of its target temperature range."""
        return self.send_notification(
            notification_type=NOTIFICATION_TYPE_OUT_OF_RANGE,
            researcher=researcher,
            experiment_id=experiment_id,
            measurement_id=measurement_id,
            cipher_data=cipher_data,
            async_send=async_send,
        )

    def get_stats(self) -> dict:
        """Returns aggregated notification latency and SLA compliance metrics."""
        with self._stats_lock:
            avg_latency = (self.total_latency / self.total_sent) if self.total_sent > 0 else 0.0
            min_lat = self.min_latency if self.total_sent > 0 else 0.0
            return {
                "total_sent": self.total_sent,
                "avg_latency": avg_latency,
                "min_latency": min_lat,
                "max_latency": self.max_latency,
                "sla_violations": self.sla_violations,
            }

    def log_stats_summary(self):
        """Logs a formatted summary of notification latency and SLA metrics."""
        stats = self.get_stats()
        logger.info(
            "=== Notification SLA Metrics === Sent: %d | Avg Latency: %.3fs | Min: %.3fs | Max: %.3fs | SLA Violations (>=10s): %d",
            stats["total_sent"],
            stats["avg_latency"],
            stats["min_latency"],
            stats["max_latency"],
            stats["sla_violations"],
        )

    def shutdown(self, wait: bool = True):
        """gracefully shuts down the background thread pool."""
        self.executor.shutdown(wait=wait)


# global default client instance for easy import in consumer
default_notifier = NotificationClient()
