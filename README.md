# Kraken L3 recorder

Records the **Kraken WebSocket level3 (L3) channel** to NDJSON files for later analysis or backtest. One line per message; files rotate by calendar day (UTC). Reconnects with exponential backoff on disconnect. Obtains a fresh session token from Kraken REST on each connection. Intended to run 24/7 on a machine that is always on.

**Authentication:** The L3 channel requires authentication. The script calls Kraken REST `GetWebSocketsToken` (with your API key/secret) and uses the token to subscribe. Set `api_key` and `api_secret` in config or via env. Your API key must have **"WebSocket interface - On"** (see [Kraken WebSocket auth](https://docs.kraken.com/api/docs/guides/spot-ws-auth)).

## Prerequisites

- Python 3.9+
- `pip install -r requirements.txt`

## Config

1. Copy the example config and edit:

   ```bash
   cp config.yaml.example config.yaml
   ```

2. Set `symbols` (Kraken format, e.g. `BTC/USD`, `ETH/USD`), `depth` (10, 100, or 1000), and `output_dir` (default `data/`). The output directory is created if missing.

3. **Required for L3:** Set Kraken API credentials. In `config.yaml` add `api_key` and `api_secret`, or use environment variables (preferred so secrets are not on disk).

You can override via environment:

- `KRAKEN_L3_SYMBOLS` — comma-separated list (e.g. `BTC/USD,ETH/USD`)
- `KRAKEN_L3_OUTPUT_DIR` — output directory path
- `KRAKEN_API_KEY`, `KRAKEN_API_SECRET` — for authenticated L3 channel

## Run

```bash
python record_l3.py
```

The script fetches a WebSocket token from Kraken REST, connects to `wss://ws-l3.kraken.com/v2`, subscribes to the `level3` channel for the configured symbols and depth, and appends each message as one NDJSON line to a file. File names are `l3-YYYY-MM-DD.ndjson` (UTC date). On disconnect it reconnects after a short backoff (1s, 2s, 4s, … up to 60s) and requests a new token. Use Ctrl+C or SIGTERM for a clean shutdown (flushes and closes the current file).

## Output

- **Path:** `{output_dir}/l3-YYYY-MM-DD.ndjson`
- **Format:** One JSON message per line (newline-delimited JSON). Each line is the raw message from Kraken: `snapshot` (full book with order-level bids/asks) or `update` (add/modify/delete order events).
- Files are appended to throughout the day; the next day a new file is opened.

## Running as a service (systemd)

Create a user unit so the recorder runs on boot and restarts on failure.

1. Install the repo and deps (e.g. in `~/github.com/coinbase-l3-recorder` with a venv or system pip).

2. Create `~/.config/systemd/user/kraken-l3-recorder.service`:

   ```ini
   [Unit]
   Description=Kraken L3 WebSocket recorder
   After=network-online.target
   Wants=network-online.target

   [Service]
   Type=simple
   WorkingDirectory=%h/github.com/coinbase-l3-recorder
   ExecStart=%h/github.com/coinbase-l3-recorder/venv/bin/python record_l3.py
   Restart=always
   RestartSec=10

   [Install]
   WantedBy=default.target
   ```

   Adjust `ExecStart` if you use a different path or `python3` from the system.

3. Enable and start (user session):

   ```bash
   systemctl --user daemon-reload
   systemctl --user enable kraken-l3-recorder
   systemctl --user start kraken-l3-recorder
   systemctl --user status kraken-l3-recorder
   ```

Logs: `journalctl --user -u kraken-l3-recorder -f`

## Data format and Crank/Mentat

Each line in the NDJSON file is one Kraken L3 message: `snapshot` (full order-level book) or `update` (add/modify/delete events per order). This is **order-level (L3) data**.

**Crank and Mentat** (e.g. Crank’s DATA_CONTRACTS) may expect **snapshot-style Parquet**: one row per snapshot with columns `timestamp`, `bid_prices`, `bid_qtys`, `ask_prices`, `ask_qtys`. To use this L3 stream for backtest or research you can add a pipeline that:

1. Rebuilds the order book from the stream (apply snapshot, then update add/modify/delete).
2. Optionally emits snapshot rows (e.g. on a time interval or on every book change) in your target schema.

This recorder only captures the raw stream; book reconstruction and Parquet export are out of scope and can be added as a separate script or in Mentat.

## References

- [Kraken WebSocket v2 – Orders (Level 3)](https://docs.kraken.com/api/docs/websocket-v2/level3/)
- [Spot Level 3 Market Data guide](https://docs.kraken.com/api/docs/guides/spot-l3-data)
- [Spot WebSockets Authentication](https://docs.kraken.com/api/docs/guides/spot-ws-auth)
- [GetWebSocketsToken REST](https://docs.kraken.com/api/docs/rest-api/get-websockets-token)
