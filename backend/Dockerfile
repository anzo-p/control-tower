FROM rust:1.69.0 as builder

USER root
RUN cargo new --bin backend
WORKDIR /backend

COPY ./Cargo.lock ./Cargo.toml ./

# Cache dependencies
RUN cargo build --release && \
    rm -f src/*.rs

COPY ./src ./src

RUN rm -f ./target/release/deps/backend* && \
    cargo build --release

FROM debian:bullseye-20240211-slim

# OpenSSL
RUN apt-get update &&  \
    apt-get install -y libssl1.1 ca-certificates &&  \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /backend/target/release/backend .

EXPOSE 3030

CMD ["./backend"]
