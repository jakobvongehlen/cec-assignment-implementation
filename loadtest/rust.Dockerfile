# Builds one of the Rust tools of the workspace (PACKAGE=experiment-producer or
# http-load-generator) from this repository, for the load test.
FROM rust:1.88-bookworm AS builder
ARG PACKAGE
RUN apt-get update && apt-get install -y --no-install-recommends cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .sqlx .sqlx
COPY event-hash event-hash
COPY experiment-producer experiment-producer
COPY http-load-generator http-load-generator
COPY notifications-service notifications-service
COPY notifier notifier
COPY test-to-api test-to-api
ENV SQLX_OFFLINE=true
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,id=cec-loadtest-target,sharing=locked \
    cargo build --release --locked -p "${PACKAGE}" \
    && cp "target/release/${PACKAGE}" /usr/local/bin/app

FROM debian:bookworm-slim
ARG PACKAGE
RUN apt-get update && apt-get install -y --no-install-recommends libssl3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /usr/local/bin/app /usr/local/bin/app
# the producer reads its schemas and .env relative to the working directory
COPY --from=builder /src/${PACKAGE} /app/${PACKAGE}
ENTRYPOINT ["app"]
