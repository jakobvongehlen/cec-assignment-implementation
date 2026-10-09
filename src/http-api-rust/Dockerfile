FROM rust:1.90-slim-bookworm AS builder

WORKDIR /usr/src/app

# build the dependencies first, so code changes don't rebuild them
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
RUN touch src/main.rs && cargo build --release --locked

FROM debian:bookworm-slim

COPY --from=builder /usr/src/app/target/release/http-api /usr/local/bin/http-api

EXPOSE 3003

ENTRYPOINT ["http-api"]
