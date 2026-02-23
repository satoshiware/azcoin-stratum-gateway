# syntax=docker/dockerfile:1

ARG VERSION=0.1.2
ARG VCS_REF=unknown

FROM rust:bookworm AS builder
ARG VERSION
ARG VCS_REF
WORKDIR /app

COPY Cargo.toml Cargo.lock VERSION ./
COPY crates ./crates

RUN cargo build --release --locked -p gateway_v1

FROM debian:bookworm-slim AS runtime
ARG VERSION
ARG VCS_REF

LABEL org.opencontainers.image.title="stratum-gateway" \
    org.opencontainers.image.version="${VERSION}" \
    org.opencontainers.image.revision="${VCS_REF}" \
    org.opencontainers.image.source="https://github.com/satoshiware/azcoin-stratum-gateway"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin appuser

WORKDIR /app

COPY --from=builder /app/target/release/gateway_v1 /usr/local/bin/gateway_v1
COPY --from=builder /app/VERSION /app/VERSION
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

ENV HEALTH_LOG_INTERVAL_SECS=30 \
    GATEWAY_VERSION_FILE=/app/VERSION \
    GATEWAY_VCS_REF=${VCS_REF}

RUN sed -i 's/\r$//' /usr/local/bin/docker-entrypoint.sh \
    && chmod +x /usr/local/bin/docker-entrypoint.sh /usr/local/bin/gateway_v1

EXPOSE 3333
USER appuser

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
