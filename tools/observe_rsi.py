"""
observe_rsi.py — Read-only RSI observation tool for the equity mean-reversion strategy.

Lists every NIFTY-50 stock's current RSI(14) and flags entry / exit candidates,
using the EXACT same logic as strategies/equity_mean_reversion.py
(entry when RSI < rsi_entry, exit zone when RSI > rsi_exit).

It places NO orders and writes nothing to the DB — pure observation.

Run (inside the backend container):
    docker compose exec backend python -m tools.observe_rsi
or locally with deps installed (from the repo root):
    python -m tools.observe_rsi
"""

from datetime import datetime, timedelta

import ta

from config import config
from data.equity_data import EquityData


def main() -> None:
    cfg = config.equity
    rsi_entry = cfg.rsi_entry   # default 30  → BUY (oversold)
    rsi_exit = cfg.rsi_exit     # default 55  → SELL / exit zone

    print("=" * 64)
    print("  FinAgent — RSI observation (equity mean-reversion)")
    print(f"  Entry: RSI < {rsi_entry:.0f}   |   Exit zone: RSI > {rsi_exit:.0f}")
    print(f"  {datetime.now():%Y-%m-%d %H:%M}")
    print("=" * 64)

    eq = EquityData()
    # ~90 calendar days gives enough trading days for a stable RSI(14)
    frm = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    universe = eq.get_nifty_universe(from_date=frm)

    rows = []
    no_data = 0
    for symbol, df in universe.items():
        if df is None or len(df) < 20:
            no_data += 1
            continue
        try:
            rsi_series = ta.momentum.RSIIndicator(df["Close"], window=14).rsi()
            rsi = float(rsi_series.iloc[-1])
            close = float(df["Close"].iloc[-1])
            rows.append((symbol, close, rsi))
        except Exception as e:  # noqa: BLE001 — observation tool, never crash
            print(f"  ! {symbol}: {e}")

    if not rows:
        print()
        print("  No equity data returned for ANY symbol.")
        print("  → Dhan Data API is almost certainly NOT subscribed yet")
        print("    (error DH-902 / 451). Subscribe to the Data API on your")
        print("    Dhan account, then re-run this command.")
        print("=" * 64)
        return

    rows.sort(key=lambda r: r[2])  # ascending RSI — most oversold first

    print(f"{'SYMBOL':<14}{'CLOSE':>12}{'RSI':>8}   SIGNAL")
    print("-" * 64)
    n_entry = n_exit = 0
    for symbol, close, rsi in rows:
        if rsi < rsi_entry:
            flag = "🟢 ENTRY (oversold — BUY)"
            n_entry += 1
        elif rsi > rsi_exit:
            flag = "🔴 EXIT ZONE (would close if held)"
            n_exit += 1
        else:
            flag = "·  watch"
        print(f"{symbol:<14}{close:>12,.1f}{rsi:>8.1f}   {flag}")

    print("-" * 64)
    print(f"  Scanned: {len(rows)} stocks   |   no-data: {no_data}")
    print(f"  🟢 Entry candidates (RSI < {rsi_entry:.0f}): {n_entry}")
    print(f"  🔴 In exit zone (RSI > {rsi_exit:.0f}): {n_exit}")
    if n_entry:
        print("  → These would generate BUY signals on the next scan.")
    else:
        print("  → No oversold stocks right now — no equity entries expected.")
    print("=" * 64)


if __name__ == "__main__":
    main()
