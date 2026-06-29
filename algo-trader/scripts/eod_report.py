"""Standalone EOD P&L report generator (suspenders for the paper session).

The paper session runner publishes its own EOD report at ~15:35; THIS script
runs from a separate systemd timer at 15:45 so the operator gets the daily P&L
even if the session process died early — it reads whatever is in the paper
store and generates + publishes from that. Idempotent (re-renders the same
reports/daily/<date>.{md,html}).

PAPER-ONLY: reads the local SQLite paper store; never touches the broker.

Usage: .venv/bin/python scripts/eod_report.py [--date YYYY-MM-DD]
                                              [--accounts a,b,c] [--no-publish]
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

PROJECT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT))

from algotrader.reports.eod import generate_eod_report, publish  # noqa: E402

IST = ZoneInfo("Asia/Kolkata")
DEFAULT_ACCOUNTS = ["paper", "paper-fut-c", "paper-opt-c", "paper-0dte"]
STORE_DIR = PROJECT / "data" / "paper"


def _accounts_with_data(accounts: list[str]) -> list[str]:
    """Only include accounts whose store DB exists, so a not-yet-wired account
    (e.g. paper-0dte before its first run) never breaks report generation."""
    present = [a for a in accounts if (STORE_DIR / f"{a}.db").exists()]
    return present or accounts[:1]


def _send_to_telegram(md_path: str) -> None:
    """Send the EOD report to the MAIN alert bot (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)
    — deliberately the alert bot, NOT the logger bot, so daily P&L lands on the clean
    reports channel. No-ops if creds are missing; never raises."""
    import os
    try:
        import requests
        from dotenv import load_dotenv
    except Exception:
        return
    load_dotenv(PROJECT / ".env")
    tok = os.getenv("TELEGRAM_BOT_TOKEN", "")
    cid = os.getenv("TELEGRAM_CHAT_ID", "")
    if not tok or not cid:
        print("telegram: main bot not configured (TELEGRAM_BOT_TOKEN/CHAT_ID) — skipping")
        return
    try:
        text = Path(md_path).read_text(encoding="utf-8")
    except Exception as e:
        print(f"telegram: could not read report: {e}")
        return
    if len(text) > 3900:  # Telegram hard limit is 4096
        text = text[:3900] + "\n... (truncated - full report on VM)"
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{tok}/sendMessage",
            data={"chat_id": cid, "text": text, "disable_web_page_preview": True},
            timeout=15,
        )
        print(f"telegram EOD report sent: {r.status_code == 200}")
    except Exception as e:
        print(f"telegram send failed: {e}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Standalone EOD P&L report.")
    p.add_argument("--date", help="YYYY-MM-DD (default: today IST)")
    p.add_argument("--accounts", help="comma-separated (default: all paper accounts present)")
    p.add_argument("--no-publish", action="store_true", help="write local only")
    p.add_argument("--no-telegram", action="store_true", help="do not send to the main Telegram bot")
    args = p.parse_args(argv)

    session_date = (date.fromisoformat(args.date) if args.date
                    else datetime.now(IST).date())
    accounts = (args.accounts.split(",") if args.accounts
                else _accounts_with_data(DEFAULT_ACCOUNTS))

    md_path, html_path = generate_eod_report(session_date, accounts)
    print(f"EOD report: {md_path} | {html_path} | accounts={accounts}")
    if not args.no_telegram:
        _send_to_telegram(md_path)
    if not args.no_publish:
        url = publish(html_path)
        print(f"published: {url}" if url else "publish FAILED (local report still written)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
