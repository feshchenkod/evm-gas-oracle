# EVM Gas Oracle

A self-hosted service written in Go that reports gas-price percentiles for EVM networks. Run one instance per network.

## Run

```bash
ETHEREUM_URL=http://your-node:8545 docker compose up -d --build
curl http://127.0.0.1:8080/
```

Set variables in your shell, in `.env`, or edit `docker-compose.yml` directly. The default RPC address points to the Docker host; its RPC must be reachable from containers.

To deploy or update the published image:

```bash
docker compose pull oracle
docker compose up -d --no-build oracle
```

Image: `ghcr.io/feshchenkod/evm-gas-oracle:latest`.

## Configuration

| Variable | Compose default | Purpose |
| --- | --- | --- |
| `ETHEREUM_URL` | `http://host.docker.internal:8545` | HTTP(S) JSON-RPC endpoint |
| `POLLING_FREQUENCY_SECONDS` | `5` | Delay between polling attempts |
| `ZERO_GAS_PRICE_POLICY` | `skip-block` | Skip blocks containing zero-price transactions, or `ignore` those transactions |
| `RPC_MAX_REQUESTS_PER_SECOND` | `5` | Maximum RPC request start rate per instance |
| `PORT` | `8080` | Published host port; internal port is 80 |
| `BIND_ADDR` | `127.0.0.1` | Published host interface |

**For BSC, set `ZERO_GAS_PRICE_POLICY=ignore`** to use paid transactions from blocks that also contain zero-price transactions. The default `skip-block` policy skips those blocks entirely.

Additional timeout, staleness and memory settings are listed in [.env.example](.env.example). If you increase `RPC_MAX_RESPONSE_BYTES`, increase both `GOMEMLIMIT` and `MEMORY_LIMIT` accordingly.

HTTP clients read a cached snapshot and do not trigger RPC requests. Each instance makes at most one RPC request at a time, within the configured rate limit. Initial synchronization takes about 10 seconds at the default limit, plus RPC latency.

## API

| Endpoint | Response |
| --- | --- |
| `/`, `/health` | Gas-price snapshot; HTTP 500 until the first sample is available |
| `/livez` | HTTP 200 while the HTTP service is running |
| `/readyz` | HTTP 200 when data is ready and fresh; otherwise 503 |

The snapshot contains `number_of_blocks`, `latest_block_number` and 28 `percentile_*` fields: 1–4, 5–95 in steps of five, and 96–100. Prices are strings such as `"0.05 nanoeth"` (1 nanoeth = 1 gwei).

Each eligible block contributes its minimum transaction gas price, excluding transactions sent by the block's miner. Percentiles use the last 200 eligible blocks. These are observed prices, not an inclusion guarantee or an EIP-1559 fee forecast.

During RPC failures, the API keeps serving its last complete snapshot. Use `/readyz` for monitoring freshness; the Docker healthcheck uses it too.

The service uses HTTP and binds to host loopback by default. Use a TLS reverse proxy when exposing it publicly.
