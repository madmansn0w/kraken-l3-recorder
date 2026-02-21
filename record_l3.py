#!/usr/bin/env python3
"""
Kraken L3 (level3) WebSocket recorder.

Retrieves a session token from Kraken REST (GetWebSocketsToken), connects to
wss://ws-l3.kraken.com/v2, subscribes to the level3 channel for configured
symbols and depth, and writes each message as a single NDJSON line. Rotates
output file by calendar day (UTC) and optionally by max file size. Reconnects with exponential backoff on
disconnect. Refreshes the token on each connection (tokens are valid 15 minutes
but do not expire while the connection is maintained). Handles SIGTERM/SIGINT
for clean shutdown.

Requires Kraken API key with "WebSocket interface - On". Set api_key and
api_secret in config or via KRAKEN_API_KEY, KRAKEN_API_SECRET.

Usage:
  pip install -r requirements.txt
  cp config.yaml.example config.yaml   # edit symbols, output_dir, auth
  python record_l3.py
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import signal
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests
import websockets
import yaml

# Kraken REST (token) and WebSocket L3 endpoints.
REST_BASE = "https://api.kraken.com"
GET_WS_TOKEN_PATH = "/0/private/GetWebSocketsToken"
WS_L3_URL = "wss://ws-l3.kraken.com/v2"

CONFIG_PATH = Path("config.yaml")
BACKOFF_INIT = 1.0
BACKOFF_MAX = 60.0
BACKOFF_MULT = 2.0
FLUSH_EVERY_N = 100

# Token is valid 15 min; we get a new one on each reconnect.
TOKEN_VALIDITY_SEC = 900


def load_config() -> dict:
    """Load config from config.yaml or env; fall back to defaults."""
    defaults = {
        "symbols": ["BTC/USD", "ETH/USD"],
        "depth": 10,
        "output_dir": "data",
        "max_file_size_mb": None,
        "api_key": None,
        "api_secret": None,
    }
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            data = yaml.safe_load(f) or {}
        defaults.update(data)
    symbols_env = os.environ.get("KRAKEN_L3_SYMBOLS")
    if symbols_env:
        defaults["symbols"] = [s.strip() for s in symbols_env.split(",")]
    output_dir = os.environ.get("KRAKEN_L3_OUTPUT_DIR")
    if output_dir:
        defaults["output_dir"] = output_dir
    max_mb_env = os.environ.get("KRAKEN_L3_MAX_FILE_SIZE_MB")
    if max_mb_env is not None:
        try:
            defaults["max_file_size_mb"] = int(max_mb_env)
        except ValueError:
            pass
    if os.environ.get("KRAKEN_API_KEY"):
        defaults["api_key"] = os.environ.get("KRAKEN_API_KEY")
    if os.environ.get("KRAKEN_API_SECRET"):
        defaults["api_secret"] = os.environ.get("KRAKEN_API_SECRET")
    return defaults


def kraken_signature(urlpath: str, data: dict, secret_b64: str) -> str:
    """
    Kraken REST API-Sign: HMAC-SHA512( urlpath + SHA256(nonce + post_data), base64_decode(secret) ), base64.
    """
    post_data = urllib.parse.urlencode(data)
    nonce = str(data["nonce"])
    encoded = (nonce + post_data).encode("utf-8")
    message = urlpath.encode("utf-8") + hashlib.sha256(encoded).digest()
    secret = base64.b64decode(secret_b64)
    mac = hmac.new(secret, message, hashlib.sha512)
    return base64.b64encode(mac.digest()).decode("utf-8")


def get_websockets_token(api_key: str, api_secret: str) -> str:
    """
    Request a WebSocket session token from Kraken REST.
    Raises on HTTP or API error; returns the token string.
    """
    nonce = str(int(time.time() * 1000))
    data = {"nonce": nonce}
    urlpath = GET_WS_TOKEN_PATH
    signature = kraken_signature(urlpath, data, api_secret)
    url = REST_BASE + urlpath
    headers = {
        "API-Key": api_key,
        "API-Sign": signature,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    resp = requests.post(url, data=data, headers=headers, timeout=15)
    resp.raise_for_status()
    out = resp.json()
    if out.get("error") and out["error"]:
        raise RuntimeError(f"Kraken API error: {out['error']}")
    token = (out.get("result") or {}).get("token")
    if not token:
        raise RuntimeError("Kraken GetWebSocketsToken did not return a token")
    return token


def date_prefix() -> str:
    """Current date in UTC for filename (YYYY-MM-DD)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class Recorder:
    def __init__(
        self,
        symbols: list[str],
        depth: int,
        output_dir: str,
        api_key: str | None,
        api_secret: str | None,
        max_file_size_mb: int | None = None,
    ):
        self.symbols = symbols
        self.depth = depth
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._api_key = api_key
        self._api_secret = api_secret
        self._max_file_size_bytes: int | None = (
            int(max_file_size_mb * 1024 * 1024) if max_file_size_mb is not None else None
        )
        self._current_date: str | None = None
        self._current_file_path: Path | None = None
        self._file = None
        self._write_count = 0
        self._shutdown = False
        self._ws = None

    def _next_path_for_date(self, date_str: str) -> Path:
        """
        Return the next available output path for the given date (YYYY-MM-DD).
        First file of the day uses l3-YYYY-MM-DD.ndjson; subsequent (size-rotated)
        files use l3-YYYY-MM-DD-0001.ndjson, l3-YYYY-MM-DD-0002.ndjson, etc.
        """
        base = self.output_dir / f"l3-{date_str}.ndjson"
        if not base.exists():
            return base
        n = 1
        while (self.output_dir / f"l3-{date_str}-{n:04d}.ndjson").exists():
            n += 1
        return self.output_dir / f"l3-{date_str}-{n:04d}.ndjson"

    def _open_file(self) -> None:
        today = date_prefix()
        # Rotate if date changed.
        if self._file is not None and self._current_date != today:
            self._file.close()
            self._file = None
            self._current_file_path = None
            self._current_date = None
        # Rotate if current file has reached max size.
        if (
            self._file is not None
            and self._current_file_path is not None
            and self._max_file_size_bytes is not None
        ):
            try:
                current_size = os.path.getsize(self._current_file_path)
                if current_size >= self._max_file_size_bytes:
                    self._file.close()
                    self._file = None
                    self._current_file_path = None
            except OSError:
                pass
        if self._file is not None:
            return
        self._current_date = today
        path = self._next_path_for_date(today)
        self._file = open(path, "a", encoding="utf-8")
        self._current_file_path = path

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
        self._current_file_path = None

    async def run(self) -> None:
        backoff = BACKOFF_INIT
        while not self._shutdown:
            try:
                if not self._api_key or not self._api_secret:
                    print(
                        "Error: Kraken L3 requires api_key and api_secret. Set in config or KRAKEN_API_KEY, KRAKEN_API_SECRET.",
                        file=sys.stderr,
                    )
                    await asyncio.sleep(min(backoff, BACKOFF_MAX))
                    backoff = min(backoff * BACKOFF_MULT, BACKOFF_MAX)
                    continue
                token = get_websockets_token(self._api_key, self._api_secret)
                async with websockets.connect(
                    WS_L3_URL,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    subscribe = {
                        "method": "subscribe",
                        "params": {
                            "channel": "level3",
                            "symbol": self.symbols,
                            "depth": self.depth,
                            "snapshot": True,
                            "token": token,
                        },
                    }
                    await ws.send(json.dumps(subscribe))
                    print(
                        f"Subscribed to level3 symbols={self.symbols} depth={self.depth}.",
                        file=sys.stderr,
                    )
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
    symbols = config["symbols"]
    depth = config["depth"]
    output_dir = config["output_dir"]
    if not symbols:
        print("No symbols configured.", file=sys.stderr)
        sys.exit(1)
    if depth not in (10, 100, 1000):
        print("depth must be 10, 100, or 1000.", file=sys.stderr)
        sys.exit(1)

    if not config.get("api_key") or not config.get("api_secret"):
        print(
            "Warning: No Kraken API key or secret set. L3 channel requires authentication. "
            "Set api_key and api_secret in config or KRAKEN_API_KEY, KRAKEN_API_SECRET.",
            file=sys.stderr,
        )

    recorder = Recorder(
        symbols=symbols,
        depth=depth,
        output_dir=output_dir,
        api_key=config.get("api_key"),
        api_secret=config.get("api_secret"),
        max_file_size_mb=config.get("max_file_size_mb"),
    )

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
        pass

    try:
        loop.run_until_complete(recorder.run())
    except KeyboardInterrupt:
        recorder._shutdown = True
        recorder.close()
    finally:
        loop.close()


if __name__ == "__main__":
    main()
