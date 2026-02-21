# Coinbase L3 recorder

Records the **Coinbase Exchange WebSocket full (L3) channel** to NDJSON files for later analysis or backtest. One line per message; files rotate by calendar day (UTC). Reconnects with exponential backoff on disconnect. Intended to run 24/7 on a machine that is always on.

## Prerequisites

- Python 3.9+
- `pip install -r requirements.txt`

## Config

1. Copy the example config and edit:

   ```bash
   cp config.yaml.example config.yaml
   ```

2. Set `product_ids` (Coinbase format, e.g. `BTC-USD`, `ETH-USD`) and `output_dir` (default `data/`). The output directory is created if missing.

You can override via environment:

- `COINBASE_L3_PRODUCT_IDS` — comma-separated list (e.g. `BTC-USD,ETH-USD`)
- `COINBASE_L3_OUTPUT_DIR` — output directory path

## Run

```bash
python record_l3.py
```

The script connects to `wss://ws-feed.exchange.coinbase.com`, subscribes to the `full` channel for the configured products, and appends each message as one NDJSON line to a file. File names are `l3-YYYY-MM-DD.ndjson` (UTC date). On disconnect it reconnects after a short backoff (1s, 2s, 4s, … up to 60s). Use Ctrl+C or SIGTERM for a clean shutdown (flushes and closes the current file).

## Output

- **Path:** `{output_dir}/l3-YYYY-MM-DD.ndjson`
- **Format:** One JSON message per line (newline-delimited JSON). Each line is the raw message from Coinbase (e.g. `snapshot`, `l2update`, `received`, `open`, `done`, `match`).
- Files are appended to throughout the day; the next day a new file is opened.

## Running as a service (systemd)

Create a user unit so the recorder runs on boot and restarts on failure.

1. Install the repo and deps (e.g. in `~/github.com/coinbase-l3-recorder` with a venv or system pip).

2. Create `~/.config/systemd/user/coinbase-l3-recorder.service`:

   ```ini
   [Unit]
   Description=Coinbase L3 WebSocket recorder
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
   systemctl --user enable coinbase-l3-recorder
   systemctl --user start coinbase-l3-recorder
   systemctl --user status coinbase-l3-recorder
   ```

Logs: `journalctl --user -u coinbase-l3-recorder -f`

## References

- [Coinbase Exchange WebSocket feed](https://docs.cdp.coinbase.com/exchange/websocket-feed/overview)
- [Channels (full = L3)](https://docs.cdp.coinbase.com/exchange/websocket-feed/channels)
