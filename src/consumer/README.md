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
