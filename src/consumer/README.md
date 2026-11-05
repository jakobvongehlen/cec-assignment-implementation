# Single-Consumer

For running a single consumer, you need to build and run two containers: one for the actual consumer and one for the database.

### Database Container

We first create a network to which the consumer(s) and the database container will connect using the following command:

```docker
docker network create cec-net
```

The database container was created using the following command:

```docker
docker run -d --network cec-net --name mypg -e POSTGRES_PASSWORD=cec -e POSTGRES_DB=temp_db -p 5432:5432 postgres:16
```

The above command automatically pulls the `postgres:16` image, creates the `mypg` container, which contains the database with the specified password and database name, and connects the default port on which PostgreSQL listens to, to a port on the host machine. We use the same port number for convenience.

We then created our main table, which holds the measurements for the experiments (only the averages), using the following command:

```docker
docker exec -it mypg psql -U postgres -d temp_db
```

This activates a command-line interface for interacting with the `temp_db` database. Subsequently, we created the aforementioned table with the following SQL query:

```sql
CREATE TABLE measurements (
    measurement_id TEXT PRIMARY KEY,
    experiment_id TEXT,
    timestamp DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    measurement_hash TEXT,
    out_of_range BOOLEAN
);
```

All SQL queries that will be used for this assignment will be stored in the `db_queries.sql` file.

Finally, we start the database container by typing:

```docker
docker start mypg
```

**IMPORTANT:** Start the database container before running any consumer.

### Single-Consumer Container

To run the single-consumer container, we first need to build the image by executing the following command from the root of the project:

```docker
docker build -t sci src/consumer
```

After building the single-consumer image (`sci`), we run the consumer as follows:

```docker
docker run --rm --name consumer_test -v "$(pwd)/auth":/usr/src/app/auth --network cec-net sci single_consumer.py "group11"
```

Finally, to test our single consumer, we can run a producer container to send messages containing experiment data in many different ways. One of these is the following:

```docker
docker run --rm -e RUST_LOG=debug --name producer -v ./auth:/experiment-producer/auth -v ./loads/2.json:/config.json -v ./logs:/logs -it dclandau/cec-experiment-producer -b kafka.cec.dlandau.nl:19092 --topic group11 --config-file /config.json --file-subscriber
```

With the above command, we can also double-check the results.

---

### Notifications Service (Interface 3)

The single-consumer integrates `notifier.py` to notify the Notifications Service via HTTP POST requests whenever temperature thresholds trigger alerts:
1. **`Stabilized`**: Sent during the `stabilization_started` phase when the average temperature first reaches the experiment's target range `[lower_threshold, upper_threshold]`.
2. **`OutOfRange`**: Sent during the `experiment_started` phase whenever the average temperature exceeds the upper threshold or drops below the lower threshold.

Notifications are dispatched asynchronously via a background thread pool (`ThreadPoolExecutor`), ensuring that the Kafka consumer loop is never blocked by HTTP calls and keeping alert latency well within the required 10-second SLA (~0.11s measured).

#### Configuration & Environment Variables

- `NOTIFICATIONS_HOST`: Target URL of the notifications service (default: `http://localhost:3000`).
- `NOTIFICATIONS_TOKEN`: Optional auth token override.
  - **Local Service**: Automatically omitted (the local container does not accept tokens).
  - **Diogo's Remote Service** (`https://notifications.cec.dlandau.nl`): Automatically reads token from `./auth/token` and appends `?token=<jwt>`.

---

### Two Modes of Running the Notifier

Depending on whether we are testing locally or preparing for Diogo's evaluation, there are two usage workflows:

#### Mode 1: Local Development & Testing (Our Deployed Notification Service)
We run our own local notification service container on the `cec-net` network and produce test experiments to `group11`:

1. **Start our local notification service container**:
   ```bash
   docker run -d --name notifications-service --network cec-net -p 3000:3000 dclandau/cec-notifications-service
   ```

2. **Run the consumer pointing to the local notification service**:
   ```bash
   docker run --rm --name consumer_test \
     -v "$(pwd)/auth":/usr/src/app/auth \
     --network cec-net \
     -e NOTIFICATIONS_HOST="http://notifications-service:3000" \
     sci single_consumer.py "group11"
   ```

3. **Produce messages with the experiment producer**:
   ```bash
   docker run --rm -e RUST_LOG=debug --name producer \
     -v ./auth:/experiment-producer/auth \
     -v ./experiment-producer/config.json:/config.json \
     -v ./logs:/logs \
     -it dclandau/cec-experiment-producer \
     -b kafka.cec.dlandau.nl:19092 \
     --topic group11 \
     --config-file /config.json \
     --file-subscriber
   ```

4. **Verify incoming notifications**:
   ```bash
   docker logs -f notifications-service
   ```

**With the whole compose stack**: the course notifications service rejects measurements from our own producer runs (`400: Cipher text not encrypted with provided nonce and server key`), so a test on `group11` needs the local service, started with the producer's key (`SECRET_KEY` in `.env`):

```bash
CONSUMER_TOPIC=group11 NOTIFICATIONS_HOST=http://notifications-service:3000 docker compose up -d --build
docker run -d --name notifications-service --network cec-assignment-implementation_default \
  dclandau/cec-notifications-service --secret-key "$(grep -oP '^SECRET_KEY="\K[^"]+' .env)" --external-ip localhost
```

Afterwards, `docker rm -f notifications-service` and `docker compose up -d` without the two variables go back to the demo setup.

#### Mode 2: Live Evaluation / Demo (Diogo's Deployed Notification Service)
Diogo produces messages to the shared `experiments` topic, and our consumer forwards notifications directly to Diogo's cloud notification service:

1. **Run the consumer pointing to Diogo's remote service**:
   ```bash
   docker run --rm --name consumer \
     -v "$(pwd)/auth":/usr/src/app/auth \
     --network cec-net \
     -e NOTIFICATIONS_HOST="https://notifications.cec.dlandau.nl" \
     sci single_consumer.py "experiments"
   ```

> **Note**: In Mode 2, `notifier.py` automatically reads the token from `./auth/token` (mounted via `-v "$(pwd)/auth":/usr/src/app/auth`) and authenticates against `https://notifications.cec.dlandau.nl`.

---

### Scaling and Consistency

[compose.yml](../../compose.yml) runs 16 consumers, `consumer_1` to `consumer_16`, one per partition of the topic. They share the partitions through the Kafka consumer group. The producer keys every event by experiment id, so all events of an experiment are in one partition and are processed by one consumer at a time.

Normally every consumer keeps its partition. A partition only moves when its consumer is gone for longer than the 6 s session timeout (a crash that doesn't recover, the container stopped), and at startup, while the 16 consumers join the group one after another. Then the new owner has to continue where the old one stopped, without losing or repeating anything:

1. **Experiment state is in Postgres.** The `experiments` table holds the configuration, phase, the out-of-range flag of the last measurement and `last_offset`, the Kafka offset of the last event that changed the experiment. The row is only written on a change (configuration, phase, the out-of-range flag flipping), not for every measurement. A consumer that sees an experiment it doesn't know loads it from there.
2. **Offsets are only committed up to what is safe.** Readings of a measurement that is still missing sensors only live in memory. Auto-commit is off; every second, and before giving up a partition, the consumer commits the offset of the first reading of its oldest incomplete measurement. The next owner reads those readings again and completes the measurement.
3. **Reading an event twice has no effect.**
   - Events with an offset at or below the experiment's `last_offset` are skipped. Measurements after it are processed again with the state they were processed with the first time, because none of them changed it.
   - Readings are stored per sensor, so a repeated reading can't be counted twice in an average.
   - Measurements are inserted with `ON CONFLICT DO NOTHING`; when one flips the flag, together with the new experiment state in one statement.
   - Notifications are sent before that write. A crash in between sends one again, and the notifications service ignores duplicates.
4. **Partitions rarely move.**
   - Static membership: each consumer joins the group with its hostname (`consumer-1` ...) as `group.instance.id`. A consumer that restarts within the 6 s session timeout gets its own partition back, without a rebalance.
   - Cooperative-sticky assignment only moves the partitions that have to move.
   - A stopped consumer (`SIGTERM`) finishes its message and commits. Because of static membership it doesn't leave the group; its partition moves after the 6 s session timeout if it doesn't come back.
5. **A failed write stops the consumer.** If a DB write fails (after one reconnect), the consumer exits without committing and its container restarts. Whoever owns the partitions next reads the message again, so a measurement is never half stored.
6. **After downtime, the backlog is skipped.** If all consumers were down while the producer kept going, the group's committed offsets are hours behind. Replaying that would send every notification in it far too late. So each commit stores its time in the commit metadata (refreshed every 5 s, also when the offset didn't move), and a consumer that gets a partition checks it (`start_offsets`):
   - Committed less than 15 s ago: a running consumer is handing the partition over (crash, restart). Continue at the committed offset, nothing is lost.
   - Older, or without a time (committed by an older version): the group was down. Start at the first message of the last `STARTUP_MAX_AGE_SECONDS` (default 5 s). The skipped messages are counted as `too_old_at_start`. Experiments configured in the skipped part are unknown and their events skipped (`unknown_experiment`).

   The consumer group is `temperature-observability-<topic>`, one per topic, so consumers testing on `group11` never share a group (and its rebalances and offsets) with those reading `experiment`.

Notifications follow the same rules as the producer uses to decide which measurements are notified: `Stabilized` for every measurement that comes back into the range during stabilization, `OutOfRange` for every measurement that leaves it while the experiment runs. The out-of-range flag resets at the start of each phase.

**Logs** show this: each consumer logs the partitions it gets and gives up (`Assigned partitions ...`, `Revoked partitions ...`) and a warning when it skips a backlog.
