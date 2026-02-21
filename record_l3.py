#!/usr/bin/env python3
"""
Coinbase L3 (full) WebSocket recorder.

Connects to wss://ws-feed.exchange.coinbase.com, subscribes to the "full"
channel for configurable product IDs, and writes each message as a single
NDJSON line. Rotates output file by calendar day (UTC). Reconnects with
exponential backoff on disconnect. Handles SIGTERM/SIGINT for clean shutdown.

Usage:
  pip install -r requirements.txt
  cp config.yaml.example config.yaml   # edit product_ids, output_dir
  python record_l3.py
"""

import asyncio
import json
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

import websockets
import yaml

WS_URL = "wss://ws-feed.exchange.coinbase.com"
CONFIG_PATH = Path("config.yaml")
BACKOFF_INIT = 1.0
BACKOFF_MAX = 60.0
BACKOFF_MULT = 2.0
FLUSH_EVERY_N = 100


def load_config() -> dict:
    """Load config from config.yaml or env; fall back to defaults."""
    defaults = {
        "product_ids": ["BTC-USD"],
        "output_dir": "data",
    }
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            data = yaml.safe_load(f) or {}
        defaults.update(data)
    product_ids = os.environ.get("COINBASE_L3_PRODUCT_IDS")
    if product_ids:
        defaults["product_ids"] = [p.strip() for p in product_ids.split(",")]
    output_dir = os.environ.get("COINBASE_L3_OUTPUT_DIR")
    if output_dir:
        defaults["output_dir"] = output_dir
    return defaults


def date_prefix() -> str:
    """Current date in UTC for filename (YYYY-MM-DD)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class Recorder:
    def __init__(self, product_ids: list[str], output_dir: str):
        self.product_ids = product_ids
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._current_date: str | None = None
        self._file = None
        self._write_count = 0
        self._shutdown = False
        self._ws = None  # set during run(); closed on shutdown to unblock recv

    def _open_file(self) -> None:
        today = date_prefix()
        if self._file is not None and self._current_date == today:
            return
        if self._file is not None:
            self._file.close()
            self._file = None
        self._current_date = today
        path = self.output_dir / f"l3-{today}.ndjson"
        self._file = open(path, "a", encoding="utf-8")

    def write_message(self, raw: str) -> None:
        self._open_file()
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError:
            self._file.write(raw.rstrip() + "\n")
        else:
            self._file.write(json.dumps(obj, ensure_ascii=False) + "\n")
        self._write_count += 1
        if self._write_count % FLUSH_EVERY_N == 0:
            self._file.flush()

    def close(self) -> None:
        if self._file is not None:
            self._file.flush()
            self._file.close()
            self._file = None
        self._current_date = None

    async def run(self) -> None:
        backoff = BACKOFF_INIT
        while not self._shutdown:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    subscribe = {
                        "type": "subscribe",
                        "channels": [{"name": "full", "product_ids": self.product_ids}],
                    }
                    await ws.send(json.dumps(subscribe))
                    backoff = BACKOFF_INIT
                    while not self._shutdown:
                        raw = await ws.recv()
                        self.write_message(raw)
            except websockets.exceptions.ConnectionClosed as e:
                if not self._shutdown:
                    print(f"Connection closed: {e}", file=sys.stderr)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
            finally:
                self._ws = None
            if self._shutdown:
                break
            delay = min(backoff, BACKOFF_MAX)
            print(f"Reconnecting in {delay:.1f}s...", file=sys.stderr)
            await asyncio.sleep(delay)
            backoff = min(backoff * BACKOFF_MULT, BACKOFF_MAX)
        self.close()


def main() -> None:
    config = load_config()
    product_ids = config["product_ids"]
    output_dir = config["output_dir"]
    if not product_ids:
        print("No product_ids configured.", file=sys.stderr)
        sys.exit(1)

    recorder = Recorder(product_ids=product_ids, output_dir=output_dir)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def shutdown_signal():
        recorder._shutdown = True
        if recorder._ws is not None:
            asyncio.ensure_future(recorder._ws.close(), loop=loop)

    try:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, shutdown_signal)
    except (ValueError, OSError):
        pass  # Windows or unsupported

    try:
        loop.run_until_complete(recorder.run())
    except KeyboardInterrupt:
        recorder._shutdown = True
        recorder.close()
    finally:
        loop.close()


if __name__ == "__main__":
    main()
