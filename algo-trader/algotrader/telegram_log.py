"""Forward Python logging to a Telegram bot — batched, rate-limit-safe, non-blocking.

Attach once near startup (e.g. in scripts/paper_trade.py main):

    from algotrader.telegram_log import attach_telegram_logging
    attach_telegram_logging()          # level from TELEGRAM_LOG_LEVEL (default INFO)

Uses a DEDICATED logger bot if configured (keeps the verbose log stream separate
from the main trade/EOD alert bot):
    TELEGRAM_LOG_BOT_TOKEN / TELEGRAM_LOG_CHAT_ID   (preferred)
    -> falls back to TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
No-ops gracefully if no creds are found, or if TELEGRAM_LOG_ENABLE=0.

Design notes
------------
- Telegram allows ~1 msg/sec per chat; sending every log line would 429. So
  records are buffered and flushed as ONE message every `flush_interval`
  seconds (or when the batch fills) — at most a few messages/minute.
- emit() only enqueues (never blocks the caller); a daemon thread does the HTTP.
- urllib3/requests/own-logger records are dropped so the handler can't feed back
  into itself (infinite loop / spam).
- Any failure is swallowed — logging must never crash the trading app.
"""
from __future__ import annotations

import atexit
import logging
import os
import threading
import time
from queue import Empty, Queue

import requests

_API = "https://api.telegram.org/bot{token}/sendMessage"
_MAX_MSG = 3500  # below Telegram's 4096 hard limit
# never forward these (HTTP chatter from the handler itself -> feedback loop)
_MUTE = ("urllib3", "requests", "httpx", "telegram_log", __name__)


class TelegramLogHandler(logging.Handler):
    def __init__(self, token: str, chat_id: str, level=logging.INFO,
                 flush_interval: float = 8.0, max_batch: int = 25):
        super().__init__(level=level)
        self.token = token
        self.chat_id = chat_id
        self.flush_interval = flush_interval
        self.max_batch = max_batch
        self._q: "Queue[str]" = Queue(maxsize=2000)
        self._session = requests.Session()
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._run, name="tg-log", daemon=True)
        self._t.start()
        atexit.register(self.close)

    def emit(self, record: logging.LogRecord) -> None:
        if record.name.startswith(_MUTE):
            return
        try:
            self._q.put_nowait(self.format(record))
        except Exception:
            pass  # queue full -> drop; logging must never raise

    def _run(self) -> None:
        buf: list[str] = []
        last = time.monotonic()
        while not self._stop.is_set():
            timeout = max(0.5, self.flush_interval - (time.monotonic() - last))
            try:
                buf.append(self._q.get(timeout=timeout))
            except Empty:
                pass
            due = (time.monotonic() - last) >= self.flush_interval
            if buf and (len(buf) >= self.max_batch or due):
                self._send(buf)
                buf, last = [], time.monotonic()
        # drain remaining on shutdown
        while True:
            try:
                buf.append(self._q.get_nowait())
            except Empty:
                break
        if buf:
            self._send(buf)

    def _send(self, lines: list[str]) -> None:
        text = "\n".join(lines)
        for i in range(0, len(text), _MAX_MSG):
            try:
                self._session.post(
                    _API.format(token=self.token),
                    data={"chat_id": self.chat_id, "text": text[i:i + _MAX_MSG],
                          "disable_web_page_preview": True},
                    timeout=10,
                )
            except Exception:
                pass  # network hiccup -> drop batch, never crash
            time.sleep(1.1)  # honour Telegram's ~1 msg/sec per-chat limit

    def close(self) -> None:
        if not self._stop.is_set():
            self._stop.set()
            self._t.join(timeout=self.flush_interval + 5)
        super().close()


_attached = None


def attach_telegram_logging(level=None, token: str | None = None,
                            chat_id: str | None = None):
    """Attach a TelegramLogHandler to the root logger (idempotent).

    Returns the handler, or None if disabled / creds missing.
    Level: arg > $TELEGRAM_LOG_LEVEL > INFO.  Set $TELEGRAM_LOG_ENABLE=0 to disable.
    """
    global _attached
    if _attached is not None:
        return _attached
    if os.getenv("TELEGRAM_LOG_ENABLE", "1").lower() in ("0", "false", "no"):
        return None
    token = token or os.getenv("TELEGRAM_LOG_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = chat_id or os.getenv("TELEGRAM_LOG_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logging.getLogger(__name__).info("Telegram logging off (no token/chat_id)")
        return None
    if level is None:
        level = os.getenv("TELEGRAM_LOG_LEVEL", "INFO")
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    h = TelegramLogHandler(token, chat_id, level=level)
    h.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%H:%M:%S"))
    root = logging.getLogger()
    root.addHandler(h)
    if root.level == logging.NOTSET or root.level > level:
        root.setLevel(level)
    _attached = h
    logging.getLogger(__name__).info(
        "Telegram logging attached @ %s", logging.getLevelName(level))
    return h
