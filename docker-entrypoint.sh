#!/bin/sh
set -eu

missing_vars=""

append_missing_var() {
  var_name="$1"
  if [ -z "$missing_vars" ]; then
    missing_vars="$var_name"
  else
    missing_vars="$missing_vars, $var_name"
  fi
}

require_non_empty() {
  var_name="$1"
  eval "var_value=\${$var_name-}"
  if [ -z "${var_value}" ]; then
    append_missing_var "$var_name"
  fi
}

# Derive bind address from host/port when not explicitly provided.
if [ -z "${GATEWAY_BIND_ADDR:-}" ]; then
  require_non_empty GATEWAY_BIND_HOST
  require_non_empty GATEWAY_BIND_PORT
  if [ -z "$missing_vars" ]; then
    GATEWAY_BIND_ADDR="${GATEWAY_BIND_HOST}:${GATEWAY_BIND_PORT}"
    export GATEWAY_BIND_ADDR
  fi
fi

# Default health interval if omitted.
if [ -z "${HEALTH_LOG_INTERVAL_SECS:-}" ]; then
  HEALTH_LOG_INTERVAL_SECS=30
  export HEALTH_LOG_INTERVAL_SECS
fi

if [ -n "$missing_vars" ]; then
  echo "[entrypoint] missing required environment variables: $missing_vars" >&2
  echo "[entrypoint] refusing to start gateway_v1" >&2
  exit 64
fi

# Fail fast on malformed values instead of silently falling back in the binary.
case "${HEALTH_LOG_INTERVAL_SECS}" in
  ''|*[!0-9]*)
    echo "[entrypoint] HEALTH_LOG_INTERVAL_SECS must be a positive integer" >&2
    exit 64
    ;;
  0)
    echo "[entrypoint] HEALTH_LOG_INTERVAL_SECS must be greater than zero" >&2
    exit 64
    ;;
esac

exec /usr/local/bin/gateway_v1
