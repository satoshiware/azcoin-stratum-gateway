# azcoin-stratum-gateway

Initial Rust scaffold for an AZCoin Stratum gateway service.

This version currently provides:
- a basic TCP listener
- a periodic health log loop
- newline-delimited JSON request handling (`id`, `method`, `params`)
- scaffold RPC methods for `mining.configure`, `mining.subscribe`, `mining.authorize`, and `mining.submit`
- an `sv2_core` foundation crate that compiles upstream SV2 dependencies (not wired yet)

## Practical repo structure

- `Cargo.toml` (workspace root)
- `Dockerfile`
- `compose.yaml`
- `README.md`
- `crates/gateway_v1/`
- `crates/sv2_core/`

## Standalone run (local Rust)

### Requirements

- Rust toolchain (stable)

### Commands

```bash
cargo run -p gateway_v1
```

Custom bind address (POSIX shells):

```bash
GATEWAY_BIND_ADDR=0.0.0.0:3333 cargo run -p gateway_v1
```

Custom health log interval (POSIX shells):

```bash
HEALTH_LOG_INTERVAL_SECS=10 cargo run -p gateway_v1
```

PowerShell examples:

```powershell
$env:GATEWAY_BIND_ADDR = "0.0.0.0:3333"; cargo run -p gateway_v1
$env:HEALTH_LOG_INTERVAL_SECS = "10"; cargo run -p gateway_v1
```

## Integrated run (Docker Compose)

Run from this repository:

```bash
docker compose -f compose.yaml up --build
```

Run in background:

```bash
docker compose -f compose.yaml up --build -d
```

Stop and remove containers:

```bash
docker compose -f compose.yaml down
```

Integrate into a larger compose stack (example command from your stack root):

```bash
docker compose -f compose.yaml -f ../azcoin-stratum-gateway/compose.yaml up --build azcoin-stratum-gateway
```

In that command, the first file is your main stack compose file and the second file is this gateway service compose file.

Runtime derivation and validation in container startup:
- if `GATEWAY_BIND_ADDR` is not set, the entrypoint derives it from `GATEWAY_BIND_HOST` and `GATEWAY_BIND_PORT`
- startup fails fast before the Rust binary launches if required derivation vars are missing
- error output lists missing variable names only (no secret values are printed)

## Ports

- `3333/tcp` default Stratum gateway listener port

## Environment variables

- `GATEWAY_BIND_ADDR` (listener address; local default: `0.0.0.0:3333`; container can derive from host/port vars)
- `GATEWAY_BIND_HOST` (compose/container derivation input, default: `0.0.0.0`)
- `GATEWAY_BIND_PORT` (compose/container derivation input, default: `3333`)
- `HEALTH_LOG_INTERVAL_SECS` (default: `30`; container startup requires a positive integer)
- `GATEWAY_PORT` (compose host port mapping, default: `3333`)
- `AZ_SHARE_SINK` (`log`, `http`, or `both`; default: `log`)
- `AZ_NODE_API_URL` (default: `http://node-api:8000`)
- `AZ_NODE_API_SHARE_PATH` (default: `/v1/mining/share`)
- `AZ_NODE_API_TOKEN` (optional bearer token for share forwarding)
- `AZ_SHARE_QUEUE_MAX` (bounded non-blocking queue size, default: `5000`)
- `AZ_SHARE_HTTP_TIMEOUT_MS` (HTTP forward timeout in milliseconds, default: `2000`)

Example compose wiring for share forwarding:

```yaml
services:
  azcoin-stratum-gateway:
    environment:
      AZ_SHARE_SINK: both
      AZ_NODE_API_URL: http://node-api:8000
      AZ_NODE_API_SHARE_PATH: /v1/mining/share
      AZ_NODE_API_TOKEN: ${AZ_NODE_API_TOKEN}
      AZ_SHARE_QUEUE_MAX: 5000
      AZ_SHARE_HTTP_TIMEOUT_MS: 2000
```

## Scaffold protocol behavior

- Input format: one JSON object per line over TCP
- Supported methods:
  - `mining.configure` -> negotiates `version-rolling` (currently accepted)
  - `mining.subscribe` -> returns scaffold subscription result
  - `mining.authorize` -> returns `true` (scaffold accept-all)
  - `mining.submit` -> MVP validation + duplicate protection (`accepted_unvalidated=true` on accepted shares)
- Unknown methods return JSON-RPC error `-32601` (`Method not found`)
- Invalid JSON does not crash the process; the line is rejected and logged
- `SHARE_EVENT` logs include both `ts` (seconds) and `ts_ms` (epoch milliseconds)
- Connection state logs include `sessions`, `jobs`, share counters (`shares_ok`, `shares_rej`, `shares_dup`), and forwarding counters (`shares_fwd_ok`, `shares_fwd_fail`, `shares_drop`)

## Quick test (PowerShell)

```powershell
$client = New-Object System.Net.Sockets.TcpClient("127.0.0.1",3333)
$stream = $client.GetStream()
$writer = New-Object System.IO.StreamWriter($stream)
$writer.AutoFlush = $true
$reader = New-Object System.IO.StreamReader($stream)

$writer.WriteLine('{"id":1,"method":"mining.subscribe","params":["test/0.1"]}')
$reader.ReadLine()

$writer.WriteLine('{"id":2,"method":"mining.authorize","params":["user.worker","x"]}')
$reader.ReadLine()
```

## Security note

- Do not commit secrets, private keys, or tokens to this repository.
