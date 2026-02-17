# syntax=docker/dockerfile:1

FROM rust:bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release --locked -p gateway_v1

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin appuser

COPY --from=builder /app/target/release/gateway_v1 /usr/local/bin/azcoin-stratum-gateway
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
    
ENV HEALTH_LOG_INTERVAL_SECS=30
    
RUN chmod +x /usr/local/bin/docker-entrypoint.sh /usr/local/bin/azcoin-stratum-gateway
    

ENV HEALTH_LOG_INTERVAL_SECS=30

RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 3333
USER appuser

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
