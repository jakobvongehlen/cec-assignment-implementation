import json
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# Dedicated logger for statistics
stats_logger = logging.getLogger("notifier_stats")
stats_logger.setLevel(logging.DEBUG)

# Robust log path handling for both Docker container (/usr/src/app) and local macOS/dev
log_dir = os.environ.get("LOG_DIR", "/usr/src/app/logs" if os.path.exists("/usr/src/app") else "./logs")
try:
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.FileHandler(os.path.join(log_dir, "notifier.log"))
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    )
    stats_logger.addHandler(file_handler)
except Exception as e:
    logger.warning("Could not setup file logger in %s: %s", log_dir, e)



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
        max_workers: Optional[int] = None,
        timeout: Optional[float] = None,
        max_retries: Optional[int] = None,
        retry_backoff: float = 0.1,
        on_latency: Optional[Callable[[float, dict], None]] = None,
    ):
        # base host can be set via env var NOTIFICATIONS_HOST
        # local default: http://localhost:3000
        # demo host: https://notifications.cec.dlandau.nl
        self.host = (host or os.environ.get("NOTIFICATIONS_HOST", "http://localhost:3000")).rstrip("/")
        self.token = token if token is not None else _resolve_token(self.host)
        workers = max_workers if max_workers is not None else int(os.environ.get("NOTIFICATIONS_MAX_WORKERS", "15"))
        self.timeout = timeout if timeout is not None else float(os.environ.get("NOTIFICATIONS_TIMEOUT", "4.0"))
        self.max_retries = max_retries if max_retries is not None else int(os.environ.get("NOTIFICATIONS_MAX_RETRIES", "2"))
        self.retry_backoff = retry_backoff
        self.executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="notifier-worker")
        self.on_latency = on_latency

        # Latency metrics & SLO tracking (< 10.0s requirement)
        self._stats_lock = threading.Lock()
        self.total_sent = 0
        self.total_latency = 0.0
        self.min_latency = float("inf")
        self.max_latency = 0.0
        self.slo_violations = 0

    def _build_url(self) -> str:
        """constructs the notification endpoint url, attaching the token query parameter if present."""
        base_url = f"{self.host}/api/notify"
        if self.token:
            query = urllib.parse.urlencode({"token": self.token})
            return f"{base_url}?{query}"
        return base_url

    def _send_request_sync(self, payload: dict) -> Optional[float]:
        """synchronously executes the http post request to the notifications endpoint with retries."""
        url = self._build_url()
        data = json.dumps(payload).encode("utf-8")

        for attempt in range(self.max_retries + 1):
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
                                    self.slo_violations += 1
    
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
                    elif status in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                        time.sleep(self.retry_backoff * (2 ** attempt))
                        continue
                    else:
                        logger.error(
                            "Notification returned unexpected status %d: %s",
                            status,
                            body,
                        )
                        return None
            except urllib.error.HTTPError as e:
                # 5xx server errors or 429 rate limit are transient and retryable under heavy load
                if e.code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                    logger.warning(
                        "Transient HTTP error %d sending notification (attempt %d/%d). Retrying in %.2fs...",
                        e.code,
                        attempt + 1,
                        self.max_retries + 1,
                        self.retry_backoff * (2 ** attempt),
                    )
                    time.sleep(self.retry_backoff * (2 ** attempt))
                    continue

                error_body = e.read().decode("utf-8", errors="replace")
                logger.error(
                    "HTTP error sending notification to %s [%d]: %s",
                    url,
                    e.code,
                    error_body,
                )
                return None
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                # Network glitches, connection resets, or timeouts are retryable
                if attempt < self.max_retries:
                    logger.warning(
                        "Network error sending notification (%s) (attempt %d/%d). Retrying in %.2fs...",
                        e,
                        attempt + 1,
                        self.max_retries + 1,
                        self.retry_backoff * (2 ** attempt),
                    )
                    time.sleep(self.retry_backoff * (2 ** attempt))
                    continue

                logger.error("Network error sending notification to %s: %s", url, e)
                return None
            except Exception as e:
                logger.error("Unexpected error sending notification to %s: %s", url, e)
                return None

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
        """Returns aggregated notification latency and SLO compliance metrics."""
        with self._stats_lock:
            avg_latency = (self.total_latency / self.total_sent) if self.total_sent > 0 else 0.0
            min_lat = self.min_latency if self.total_sent > 0 else 0.0
            return {
                "total_sent": self.total_sent,
                "avg_latency": avg_latency,
                "min_latency": min_lat,
                "max_latency": self.max_latency,
                "slo_violations": self.slo_violations,
                "sla_violations": self.slo_violations,  # alias for backwards compatibility
            }

    def log_stats_summary(self):
        """Logs a formatted summary of notification latency and SLO metrics."""
        stats = self.get_stats()
        summary_msg = (
            f"=== Notification SLA Metrics === Sent: {stats['total_sent']} | "
            f"Avg Latency: {stats['avg_latency']:.3f}s | Min: {stats['min_latency']:.3f}s | "
            f"Max: {stats['max_latency']:.3f}s | SLA Violations (>=10s): {stats['slo_violations']}"
        )
        stats_logger.info(summary_msg)
        logger.info(summary_msg)

    def shutdown(self, wait: bool = True):
        """gracefully shuts down the background thread pool."""
        self.executor.shutdown(wait=wait)


# global default client instance for easy import in consumer
default_notifier = NotificationClient()