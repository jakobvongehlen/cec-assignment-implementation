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

The above command automatically pulls the `postgres:16` image, creates the `mypg` container, which contains the database with the specified password and database name, and connects the default port on which PostgreSQL listens to a port on the host machine. We use the same port number for convenience.

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
