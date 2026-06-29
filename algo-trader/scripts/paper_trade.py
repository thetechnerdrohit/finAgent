#!/usr/bin/env python3
"""Paper-trading day runner (ARCHITECTURE §6).

Usage
-----
Live feed (market hours):
    python scripts/paper_trade.py --accounts paper paper-minlot

Replay a past date (market-closed testing):
    python scripts/paper_trade.py --accounts paper paper-minlot --replay 2026-06-10

Accounts accepts both space-separated and comma-separated values:
    --accounts paper,paper-minlot
    --accounts paper paper-minlot

Description
-----------
Orchestration layer:
  1. Parse args and mint/validate access token (live mode only).
  2. Build instrument universe:
       - NIFTY-FUT  (FnoInstrument, IDX→NSE_FNO; replay data from NIFTY index cache)
       - BANKNIFTY-FUT (same)
       - NIFTY-50 equities (for LiveBreadth only; from data/cache dirs)
  3. Wire ReplayDriver (or LiveBarFeed) → LiveBreadth → PaperExecutors.
  4. Strategy: BreadthRider cell {breadth_thr=0.72, decision_time='10:15',
       stop_atr_mult=2.0, trail_atr_mult=3.5} on each futures instrument.
  5. After session: verify LiveBreadth vs _BREADTH parquet (replay mode),
     sync trade records to functional API DB, generate EOD report.
     Publish is SKIPPED in replay mode (--replay flag or --skip-publish).

Paper-only invariant: this module never imports or calls any order-placement
method.  ALGOTRADER_LIVE_ENABLE sentinel is not checked here — paper loop
only uses market-data interfaces.
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import time as _time_mod
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

IST = ZoneInfo("Asia/Kolkata")

# NIFTY-50 equity symbols available in the data cache (for LiveBreadth)
_NIFTY50_CACHE_SYMBOLS = [
    "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJAJ-AUTO",
    "BAJAJFINSV", "BAJFINANCE", "BEL", "BHARTIARTL", "BPCL",
    "BRITANNIA", "CIPLA", "COALINDIA", "DRREDDY", "EICHERMOT",
    "ETERNAL", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE",
    "HEROMOTOCO", "HINDALCO", "HINDUNILVR", "ICICIBANK", "INDUSINDBK",
    "INFY", "ITC", "JSWSTEEL", "KOTAKBANK", "LT",
    "MARUTI", "M&M", "NESTLEIND", "NTPC", "ONGC",
    "POWERGRID", "RELIANCE", "SBILIFE", "SBIN", "SHRIRAMFIN",
    "SUNPHARMA", "TATACONSUM", "TATAMOTORS", "TATASTEEL", "TCS",
    "TECHM", "TITAN", "TRENT", "ULTRACEMCO", "WIPRO",
]

_SESSION_REPORT_AT = time(15, 35, 0)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Graceful-degradation imports
# ---------------------------------------------------------------------------

try:
    from algotrader.paper.executor import PaperExecutor, MinLotOverrideEngine, ZerodteFixedLotEngine
    _EXECUTOR_AVAILABLE = True
except ImportError:
    PaperExecutor = None  # type: ignore[assignment,misc]
    MinLotOverrideEngine = None  # type: ignore[assignment,misc]
    ZerodteFixedLotEngine = None  # type: ignore[assignment,misc]
    _EXECUTOR_AVAILABLE = False
    log.warning("PaperExecutor not available")


# ---------------------------------------------------------------------------
# Account registry (OPTION-C PAPER COMPARISON §A + §B)
# ---------------------------------------------------------------------------

#: Canonical account configurations for the paper-trading runner.
#:
#: Keys
#: ----
#: per_trade_risk_pct : float
#:     Fraction of capital risked per trade (fed to IntradayRiskEngine).
#: minlot : bool
#:     If True, wrap the risk engine with MinLotOverrideEngine so a zero_qty
#:     Rejection on derivatives is promoted to qty=1 provided the position is
#:     fundable.
#: minlot_cap_pct : float | None
#:     If set, overrides MIN_LOT_BUDGET_FACTOR on the shim to
#:     minlot_cap_pct / per_trade_risk_pct.  E.g. for paper-fut-c:
#:     0.02 / 0.0175 ≈ 1.143× — ensures the promoted lot's risk does not
#:     exceed 2 % of capital.
#: expression : "futures" | "options"
#:     "futures"  → two BreadthRider instances (NIFTY-FUT + BANKNIFTY-FUT).
#:     "options"  → one BreadthRiderOptions instance (NIFTY only; BANKNIFTY
#:                  monthly options skipped — premia too large for 0.75 %).
ACCOUNTS: dict[str, dict] = {
    "paper": {
        "per_trade_risk_pct": 0.0075,
        "minlot": False,
        "minlot_cap_pct": None,
        "expression": "futures",
    },
    "paper-minlot": {
        "per_trade_risk_pct": 0.0075,
        "minlot": True,
        "minlot_cap_pct": None,
        "expression": "futures",
    },
    # OPTION-C comparison leg A: identical futures signal, 1.75% risk, min-lot shim
    # capped at 2% effective risk per trade.
    "paper-fut-c": {
        "per_trade_risk_pct": 0.0175,
        "minlot": True,
        "minlot_cap_pct": 0.02,
        "expression": "futures",
    },
    # OPTION-C comparison leg B: options expression, 0.75% risk, no min-lot shim.
    "paper-opt-c": {
        "per_trade_risk_pct": 0.0075,
        "minlot": False,
        "minlot_cap_pct": None,
        "expression": "options",
    },
    # 0DTE expiry-day ATM short straddle (NIFTY weekly).  Fixed-1-lot sizing
    # via ZerodteFixedLotEngine; auto-fires only on NIFTY weekly expiry days.
    "paper-0dte": {
        "per_trade_risk_pct": 0.0075,
        "minlot": False,
        "minlot_cap_pct": None,
        "expression": "zerodte_straddle",
    },
}

try:
    from algotrader.data.live_feed import LiveBarFeed, ReplayDriver, BarEvent
    _FEED_AVAILABLE = True
except ImportError:
    LiveBarFeed = None   # type: ignore[assignment,misc]
    ReplayDriver = None  # type: ignore[assignment,misc]
    BarEvent = None      # type: ignore[assignment,misc]
    _FEED_AVAILABLE = False
    log.warning("live_feed not available")

try:
    from algotrader.data.rest_poll_feed import RestPollingBarSource
    _REST_FEED_AVAILABLE = True
except ImportError:
    RestPollingBarSource = None  # type: ignore[assignment,misc]
    _REST_FEED_AVAILABLE = False
    log.warning("rest_poll_feed not available")

try:
    from algotrader.data.ws_feed_v2 import DhanLiveFeedV2
    _WS_V2_AVAILABLE = True
except ImportError:
    DhanLiveFeedV2 = None  # type: ignore[assignment,misc]
    _WS_V2_AVAILABLE = False
    log.warning("ws_feed_v2 not available")

try:
    from algotrader.data.feed_manager import FeedManager
    _FEED_MGR_AVAILABLE = True
except ImportError:
    FeedManager = None  # type: ignore[assignment,misc]
    _FEED_MGR_AVAILABLE = False
    log.warning("feed_manager not available")

try:
    from algotrader.data.live_breadth import LiveBreadth
    _BREADTH_AVAILABLE = True
except ImportError:
    LiveBreadth = None   # type: ignore[assignment,misc]
    _BREADTH_AVAILABLE = False
    log.warning("LiveBreadth not available")

from algotrader.data.token_manager import get_valid_token
from algotrader.reports.eod import generate_eod_report, publish


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Paper-trading day runner.")
    parser.add_argument(
        "--accounts",
        nargs="+",
        default=["paper", "paper-fut-c", "paper-opt-c", "paper-0dte"],
        metavar="ACCOUNT_ID",
        help=(
            "Account IDs (space- or comma-separated). "
            "Default: paper,paper-fut-c,paper-opt-c (OPTION-C comparison). "
            "See ACCOUNTS registry in this module for per-account config."
        ),
    )
    parser.add_argument(
        "--replay",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="Replay stored bars for this date instead of live feed.",
    )
    parser.add_argument(
        "--skip-publish",
        action="store_true",
        default=False,
        help="Skip soma-publish (automatically set in replay mode).",
    )
    parser.add_argument(
        "--feed",
        default="auto",
        choices=["auto", "rest", "ws"],
        help=(
            "Live bar feed backend.  "
            "'auto' (default) = FeedManager: raw-WS primary (DhanLiveFeedV2) with "
            "automatic REST failover within ~90 s of bar silence — safe even if WS "
            "stalls (as it did for 6.5 h on 2026-06-15).  "
            "'rest' = RestPollingBarSource only (proven reliable).  "
            "'ws' = legacy dhanhq SDK MarketFeed (zombie-socket risk)."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    ns = parser.parse_args(argv)
    # Normalise: support both "paper,paper-minlot" and "paper paper-minlot"
    accounts: list[str] = []
    for tok in ns.accounts:
        accounts.extend(t.strip() for t in tok.split(",") if t.strip())
    ns.accounts = accounts
    return ns


# ---------------------------------------------------------------------------
# Instrument / subscription builders
# ---------------------------------------------------------------------------

def _build_futures_subs() -> tuple[list, dict[str, str]]:
    """Return (subscriptions, data_symbol_map) for NIFTY-FUT and BANKNIFTY-FUT.

    data_symbol_map maps instrument.symbol → cache directory name so the
    ReplayDriver loads index bar files for the futures instruments.
    """
    from algotrader.data.instruments import NIFTY_FUT, BANKNIFTY_FUT
    # Live REST fetches the INDEX bars (security 13/25 on IDX_I) for the
    # underlying signal; the strategy-facing instrument stays *_FUT so symbol
    # matching + futures cost/lot still apply. In replay the segment is unused
    # (ReplayDriver uses data_symbol_map for the cache dir).
    subs = [
        (NIFTY_FUT.security_id, "IDX_I", NIFTY_FUT),
        (BANKNIFTY_FUT.security_id, "IDX_I", BANKNIFTY_FUT),
    ]
    # Map futures symbols to index cache dirs (bars are identical)
    sym_map = {
        NIFTY_FUT.symbol:     "NIFTY",
        BANKNIFTY_FUT.symbol: "BANKNIFTY",
    }
    return subs, sym_map


def _build_equity_subs() -> list:
    """Return subscriptions for NIFTY-50 equities (breadth computation only).

    Uses symbol as security_id placeholder (safe in replay; not used for live
    websocket subscription in this function's primary use case).
    """
    from algotrader.backtest.data_source import equity_instrument
    cache = _PROJECT_ROOT / "data" / "cache"
    subs = []
    for sym in _NIFTY50_CACHE_SYMBOLS:
        if not (cache / sym / "1m").exists():
            log.debug("equity cache missing for %s — skipping breadth", sym)
            continue
        instr = equity_instrument(sym)   # REAL Dhan security_id from the NIFTY-50 map
        if instr.security_id in ("", "?"):
            log.warning("no security_id for %s — skipping breadth", sym)
            continue
        subs.append((instr.security_id, "NSE_EQ", instr))
    return subs


# ---------------------------------------------------------------------------
# Executor factory
# ---------------------------------------------------------------------------

def _build_executor(
    account_id: str,
    session_date: date,
    store_path: Path | None = None,
    breadth_lookup=None,
    subscribe_cb=None,
) -> "PaperExecutor":
    """Construct a PaperExecutor for *account_id* using the ACCOUNTS registry.

    The frozen BreadthRider cell is shared across all accounts:
        breadth_thr=0.72, decision_time='10:15',
        stop_atr_mult=2.0, trail_atr_mult=3.5.

    Account-specific config (risk_pct, minlot, expression) is read from the
    module-level ACCOUNTS dict; unknown account_ids fall back to the 'paper'
    defaults so legacy test accounts keep working.

    Parameters
    ----------
    subscribe_cb:
        Callable[[list[tuple[str, str, Instrument]]], None] wired to
        LiveBarFeed.subscribe_dynamic in live mode; None in replay mode.
        Only used for 'options' expression accounts.
    """
    from algotrader.backtest.costs import DhanCosts
    from algotrader.data.instruments import NIFTY_FUT, BANKNIFTY_FUT
    from algotrader.paper.executor import PaperExecutor, MinLotOverrideEngine, ZerodteFixedLotEngine
    from algotrader.paper.store import PaperStore
    from algotrader.risk.engine import IntradayRiskEngine, RiskParams
    from algotrader.strategies.breadth_rider import BreadthRider
    from algotrader.config import RiskConfig

    # Frozen BreadthRider cell (per assignment spec)
    _CELL = dict(
        breadth_thr=0.72,
        decision_time="10:15",
        stop_atr_mult=2.0,
        trail_atr_mult=3.5,
    )

    # Look up per-account config; fall back to 'paper' defaults for unknown ids.
    _fallback = ACCOUNTS.get("paper", {})
    cfg = ACCOUNTS.get(account_id, {
        "per_trade_risk_pct": _fallback.get("per_trade_risk_pct", 0.0075),
        "minlot": "minlot" in account_id,   # legacy: "paper-minlot" stays working
        "minlot_cap_pct": None,
        "expression": "futures",
    })

    expression = cfg.get("expression", "futures")
    use_minlot = cfg.get("minlot", False)
    minlot_cap_pct = cfg.get("minlot_cap_pct", None)

    rc = RiskConfig()
    per_trade_risk_pct = cfg.get("per_trade_risk_pct", rc.per_trade_risk_pct)

    # ── Strategy list ─────────────────────────────────────────────────
    if expression == "zerodte_straddle":
        from algotrader.strategies.zerodte_straddle_live import ZerodteStraddleLive
        strat = ZerodteStraddleLive(subscribe_cb=subscribe_cb)
        strategies = [strat]
    elif expression == "options":
        from algotrader.strategies.breadth_rider_options import BreadthRiderOptions
        strat = BreadthRiderOptions(
            **_CELL,
            breadth_lookup=breadth_lookup,
            subscribe_cb=subscribe_cb,
        )
        # Unique strategy_id to avoid key collision in executor._strat_by_id
        strat.strategy_id = "breadth_rider_options_nifty"
        strategies = [strat]
    else:
        # breadth_lookup wiring is SAFETY-CRITICAL: without it the strategy reads
        # the static precomputed parquet, which has no rows for a live session —
        # the 2026-06-12 session was silently blind and missed a SHORT signal.
        strategies = [
            BreadthRider(**_CELL, target_symbol=NIFTY_FUT.symbol,
                         breadth_lookup=breadth_lookup),
            BreadthRider(**_CELL, target_symbol=BANKNIFTY_FUT.symbol,
                         breadth_lookup=breadth_lookup),
        ]
        # Give each instance a unique strategy_id to avoid key collision in the
        # executor's _strat_by_id dict (both share the class-level strategy_id).
        strategies[0].strategy_id = "breadth_rider_nifty"
        strategies[1].strategy_id = "breadth_rider_banknifty"

    # ── Risk engine ───────────────────────────────────────────────────
    params = RiskParams(
        capital=rc.capital,
        hard_floor=rc.hard_floor,
        max_daily_loss_pct=rc.max_daily_loss_pct,
        per_trade_risk_pct=per_trade_risk_pct,
        max_open_positions=rc.max_open_positions,
        max_per_symbol=1,
    )
    risk = IntradayRiskEngine(params)

    if expression == "zerodte_straddle":
        # Fixed-1-lot shim for the short straddle; standard sizing is wrong
        # for this strategy (wide catastrophic stop yields 0 lots).
        risk_engine = ZerodteFixedLotEngine(risk)
    elif use_minlot:
        shim = MinLotOverrideEngine(risk)
        if minlot_cap_pct is not None and per_trade_risk_pct > 0:
            # Override the budget factor so the promoted lot's risk does not
            # exceed minlot_cap_pct of capital.  E.g. paper-fut-c: 0.02/0.0175
            # = 1.143× → effective risk cap of 2 % per trade when promoting.
            shim.MIN_LOT_BUDGET_FACTOR = minlot_cap_pct / per_trade_risk_pct
        risk_engine = shim
    else:
        risk_engine = risk

    db_path = store_path or (_PROJECT_ROOT / "data" / f"paper_{account_id}.db")
    store = PaperStore(db_path=db_path)

    return PaperExecutor(
        account_id=account_id,
        strategies=strategies,
        risk_engine=risk_engine,
        cost_model=DhanCosts(),
        store=store,
    )


# ---------------------------------------------------------------------------
# Sync PaperStore → functional API (for EOD report)
# ---------------------------------------------------------------------------

def _sync_to_fn_api(executors: list, session_date: date) -> None:
    """Write PaperExecutor trades / risk events to the functional-API DB.

    The EOD report reads from the functional API (data/paper/<account>.db).
    PaperExecutor writes to PaperStore (a separate DB).  This bridge syncs
    them after the session so EOD reporting works unchanged.
    """
    from algotrader.paper import store as _store_mod
    from datetime import datetime as _dt

    for executor in executors:
        account_id = executor.account_id
        _store_mod.init_db(account_id)

        # Sync closed trades
        raw_trades = executor._store.load_trades(account_id, session_date)
        for row in raw_trades:
            trade_for_fn = dict(row)
            # Timestamps are ISO strings in PaperStore; functional API needs datetimes
            for key in ("entry_ts", "exit_ts"):
                v = trade_for_fn[key]
                if isinstance(v, str):
                    parsed = _dt.fromisoformat(v)
                    if parsed.tzinfo is None:
                        parsed = parsed.replace(tzinfo=IST)
                    trade_for_fn[key] = parsed.astimezone(IST)
            _store_mod.record_trade(account_id, session_date, trade_for_fn)

        # Sync risk events
        for ev in executor.risk_events:
            _store_mod.record_risk_event(account_id, session_date, ev)

    log.info("Synced %d executor(s) to functional API", len(executors))


# ---------------------------------------------------------------------------
# LiveBreadth vs parquet verification
# ---------------------------------------------------------------------------

def _verify_breadth_vs_parquet(
    breadth: "LiveBreadth",
    session_date: date,
    tol: float = 0.02,
) -> None:
    """Log a per-row comparison of LiveBreadth snapshots vs the precomputed parquet.

    tol: absolute tolerance for pct_above_vwap / net_breadth match.
    Differences > tol are logged as WARNING; summary logged as INFO.
    """
    import pandas as pd
    breadth_parquet = _PROJECT_ROOT / "data" / "cache" / "_BREADTH" / "5m" / "breadth.parquet"
    if not breadth_parquet.exists():
        log.warning("_verify_breadth: parquet not found at %s", breadth_parquet)
        return

    df = pd.read_parquet(breadth_parquet)
    df["ts"] = pd.to_datetime(df["ts"])
    mask = df["ts"].dt.date == session_date
    day_df = df[mask].copy()
    if day_df.empty:
        log.warning("_verify_breadth: no rows in parquet for %s", session_date)
        return

    n_checked = 0
    n_mismatch = 0
    for _, prow in day_df.iterrows():
        epoch_s = int(prow["ts"].timestamp())
        live = breadth.at(epoch_s)
        if live is None:
            log.debug("breadth verify: no live snapshot at %s", prow["ts"])
            continue
        live_pct, live_n, live_net = live
        parq_pct = float(prow["pct_above_vwap"])
        parq_net = float(prow["net_breadth"])
        n_checked += 1
        if abs(live_pct - parq_pct) > tol or abs(live_net - parq_net) > tol:
            n_mismatch += 1
            log.warning(
                "breadth MISMATCH @%s: live_pct=%.4f parq_pct=%.4f "
                "live_net=%.4f parq_net=%.4f",
                prow["ts"].strftime("%H:%M"),
                live_pct, parq_pct, live_net, parq_net,
            )
        else:
            log.debug(
                "breadth OK @%s: pct=%.4f net=%.4f n=%d",
                prow["ts"].strftime("%H:%M"), live_pct, live_net, live_n,
            )

    log.info(
        "Breadth verification %s: %d/%d boundaries checked, %d mismatches (tol=%.3f)",
        session_date, n_checked, len(day_df), n_mismatch, tol,
    )


# ---------------------------------------------------------------------------
# Bar router
# ---------------------------------------------------------------------------

class _BarRouter:
    """Routes BarEvents from the feed to LiveBreadth and PaperExecutors.

    All bars (equity + futures + options) go to LiveBreadth.
    Only derivative bars go to PaperExecutors, with selective routing:

    - Option bars (symbol contains "-CE" or "-PE") are routed to
      **options executors** (expression == "options" or "zerodte_straddle") ONLY.
    - Futures bars are routed to **ALL** executors — both futures-only accounts
      and options/straddle accounts that need the underlying bar for entry
      signals (BreadthRiderOptions at 10:15; ZerodteStraddleLive at 09:20).

    This corrects the earlier routing limitation where options-expression
    executors were blind to the underlying futures bars they need for signals.
    Futures bars in options strategies are silently ignored for instruments the
    strategy does not track (safe: each strategy filters by symbol internally).
    """

    def __init__(
        self,
        breadth: object | None,
        executors: list,
        session_date: date,
        opt_accounts: "set[str] | None" = None,
    ) -> None:
        self._breadth   = breadth
        self._executors = executors
        self._date      = session_date
        self._bar_count = 0
        # Set of account_ids whose expression == "options"
        self._opt_accounts: set[str] = opt_accounts or set()

    @staticmethod
    def _is_option_bar(symbol: str) -> bool:
        """True if *symbol* looks like an index option (contains "-CE" or "-PE")."""
        return "-CE" in symbol or "-PE" in symbol

    def on_bar(self, event: object) -> None:
        bar = getattr(event, "bar", None)
        if bar is None:
            return
        self._bar_count += 1

        # Feed ALL bars to breadth (equity bars drive the computation)
        if self._breadth is not None:
            try:
                self._breadth.on_bar(bar.instrument.symbol, bar)
            except Exception:
                log.exception("LiveBreadth.on_bar raised")

        # Feed only DERIVATIVE bars to paper executors, with option/futures routing.
        if bar.instrument.is_derivative:
            is_opt = self._is_option_bar(bar.instrument.symbol)
            for executor in self._executors:
                is_opt_exec = executor.account_id in self._opt_accounts
                if is_opt:
                    # Option bars → options/straddle executors only.
                    if is_opt_exec:
                        try:
                            executor.on_bar(bar)
                        except Exception:
                            log.exception(
                                "PaperExecutor.on_bar raised for %s (option bar)",
                                executor.account_id,
                            )
                else:
                    # Futures bars → ALL executors (options strategies need
                    # underlying signal bars; strategies filter internally).
                    try:
                        executor.on_bar(bar)
                    except Exception:
                        log.exception(
                            "PaperExecutor.on_bar raised for %s (futures bar)",
                            executor.account_id,
                        )

        if self._bar_count % 500 == 0:
            log.debug(
                "bar_count=%d symbol=%s ts=%s",
                self._bar_count, bar.instrument.symbol, bar.ts_open,
            )


# ---------------------------------------------------------------------------
# Replay run
# ---------------------------------------------------------------------------

def _run_replay(args: argparse.Namespace, replay_date: date) -> list:
    """Replay stored bars. Returns list of PaperExecutors for post-processing."""
    if not _FEED_AVAILABLE:
        log.error("ReplayDriver not available")
        return []

    futures_subs, sym_map = _build_futures_subs()
    equity_subs = _build_equity_subs()
    eq_symbols = [instr.symbol for _, _, instr in equity_subs]

    breadth = LiveBreadth(eq_symbols) if _BREADTH_AVAILABLE else None

    executors = []
    if _EXECUTOR_AVAILABLE:
        for account_id in args.accounts:
            # In replay mode subscribe_cb is None — no live feed to wire.
            # Options accounts still build correctly; since no signal fires on
            # no-signal days (e.g. 2026-06-10, breadth=0.32) the pending state
            # is never entered, so the absence of a real feed does not matter.
            exec_ = _build_executor(
                account_id, replay_date,
                breadth_lookup=breadth.at if breadth else None,
                subscribe_cb=None,
            )
            executors.append(exec_)

    # Determine which accounts receive option bars (CE/PE routing).
    # Both 'options' and 'zerodte_straddle' expressions subscribe to option bars.
    opt_accounts = {
        aid for aid in args.accounts
        if ACCOUNTS.get(aid, {}).get("expression") in ("options", "zerodte_straddle")
    }
    router = _BarRouter(breadth, executors, replay_date, opt_accounts=opt_accounts)

    all_subs = futures_subs + equity_subs
    driver = ReplayDriver(
        subscriptions=all_subs,
        on_bar=router.on_bar,
        speed=0.0,
        data_symbol_map=sym_map,
    )

    log.info(
        "Replay %s | %d futures + %d equity instruments",
        replay_date, len(futures_subs), len(equity_subs),
    )
    driver.run(session_date=replay_date)
    log.info("Replay complete: %d bars processed", router._bar_count)

    # Verify live breadth against precomputed parquet
    if breadth is not None:
        _verify_breadth_vs_parquet(breadth, replay_date)

    return executors


# ---------------------------------------------------------------------------
# Live run
# ---------------------------------------------------------------------------

def _run_live(args: argparse.Namespace, session_date: date) -> list:
    """Run live paper session. Returns list of PaperExecutors."""
    feed_mode = getattr(args, "feed", "auto")
    if feed_mode == "auto" and not _FEED_MGR_AVAILABLE and not _REST_FEED_AVAILABLE:
        log.error("FeedManager and RestPollingBarSource both unavailable; cannot run")
        return []
    if feed_mode == "ws" and not _FEED_AVAILABLE:
        log.error("LiveBarFeed not available; cannot run live mode with --feed ws")
        return []
    if feed_mode == "rest" and not _REST_FEED_AVAILABLE and not _FEED_AVAILABLE:
        log.error("Neither RestPollingBarSource nor LiveBarFeed available; cannot run")
        return []

    token = get_valid_token()
    if not token:
        log.error("Could not obtain a valid access token — exiting")
        sys.exit(1)

    futures_subs, _ = _build_futures_subs()
    equity_subs = _build_equity_subs()
    eq_symbols = [instr.symbol for _, _, instr in equity_subs]

    breadth = LiveBreadth(eq_symbols) if _BREADTH_AVAILABLE else None

    executors = []
    if _EXECUTOR_AVAILABLE:
        for account_id in args.accounts:
            # Build executors without subscribe_cb first (feed not yet created).
            exec_ = _build_executor(
                account_id, session_date,
                breadth_lookup=breadth.at if breadth else None,
                subscribe_cb=None,
            )
            executors.append(exec_)

    # Determine which accounts receive option bars (CE/PE routing).
    opt_accounts = {
        aid for aid in args.accounts
        if ACCOUNTS.get(aid, {}).get("expression") in ("options", "zerodte_straddle")
    }
    router = _BarRouter(breadth, executors, session_date, opt_accounts=opt_accounts)

    # Subscribe EVERYTHING live: the 50 equities feed LiveBreadth.
    # REST feed: equity bars arrive via REST polling just like futures/index bars.
    # WS feed (legacy): ws supports 5000 instruments/conn but delivered zero bars
    # on 2026-06-15 (0DTE expiry) — REST is now the default.
    live_subs = futures_subs + equity_subs

    if feed_mode == "auto":
        # FeedManager: DhanLiveFeedV2 primary + automatic REST failover within 90 s.
        # Safe by design: if WS stalls (as it did on 2026-06-15), REST takes over.
        if _FEED_MGR_AVAILABLE:
            import os
            feed = FeedManager(
                subscriptions=live_subs,
                on_bar=router.on_bar,
                access_token=token,
                client_id=os.environ.get("DHAN_CLIENT_ID", ""),
                failover_seconds=90.0,
            )
            log.info(
                "Using FeedManager (DhanLiveFeedV2 primary + REST failover @ 90s)"
            )
        else:
            # Graceful degradation to REST-only
            log.warning(
                "FeedManager not available — falling back to REST-only for safety"
            )
            feed_mode = "rest"

    if feed_mode == "rest":
        if not _REST_FEED_AVAILABLE:
            log.error("RestPollingBarSource not available; falling back to ws")
            feed_mode = "ws"
        else:
            feed = RestPollingBarSource(
                subscriptions=live_subs,
                on_bar=router.on_bar,
                access_token=token,
            )
            log.info("Using REST polling feed (reliable path)")

    if feed_mode == "ws":
        if not _FEED_AVAILABLE:
            log.error("LiveBarFeed not available; cannot run live mode")
            return []
        feed = LiveBarFeed(
            subscriptions=live_subs,
            on_bar=router.on_bar,
            access_token=token,
        )
        log.info("Using WebSocket feed (legacy SDK mode — zombie-socket risk)")

    # Wire subscribe_cb to options/straddle strategies AFTER the feed is
    # created so the callback is the live feed's subscribe_dynamic method.
    try:
        from algotrader.strategies.breadth_rider_options import BreadthRiderOptions
        from algotrader.strategies.zerodte_straddle_live import ZerodteStraddleLive
        for exec_ in executors:
            for strat in exec_._strategies:
                if isinstance(strat, (BreadthRiderOptions, ZerodteStraddleLive)):
                    strat.subscribe_cb = feed.subscribe_dynamic
                    log.info(
                        "Wired subscribe_cb for %s / %s",
                        exec_.account_id, strat.strategy_id,
                    )
    except ImportError:
        log.warning("BreadthRiderOptions/ZerodteStraddleLive not available; options accounts will be inert")

    _stop = [False]
    def _handle_sig(sig, frame):  # noqa: ANN001
        log.info("signal %s received — stopping feed", sig)
        _stop[0] = True
    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT, _handle_sig)

    log.info("Starting live feed for session %s", session_date)
    feed.start()

    while not _stop[0]:
        now = datetime.now(IST)
        if now.time() >= _SESSION_REPORT_AT:
            break
        _time_mod.sleep(5)

    log.info("Stopping feed …")
    feed.stop()
    return executors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Mirror all logs to the Telegram bot (batched, rate-limit-safe, optional).
    try:
        from algotrader.telegram_log import attach_telegram_logging
        attach_telegram_logging()
    except Exception:
        log.exception("telegram logging attach failed")

    session_date: date = args.replay or datetime.now(IST).date()
    skip_publish: bool = args.skip_publish or (args.replay is not None)

    if args.replay:
        executors = _run_replay(args, args.replay)
    else:
        executors = _run_live(args, session_date)

    # Bridge PaperStore → functional API (EOD report reads from functional API)
    if executors:
        try:
            _sync_to_fn_api(executors, session_date)
        except Exception:
            log.exception("Failed to sync trades to functional API")

    # EOD report — always generated
    log.info("Generating EOD report for %s …", session_date)
    try:
        md_path, html_path = generate_eod_report(session_date, args.accounts)
        log.info("EOD report: md=%s  html=%s", md_path, html_path)
        if skip_publish:
            log.info("Publish SKIPPED (replay mode or --skip-publish)")
        else:
            url = publish(html_path)
            if url:
                log.info("Published: %s", url)
            else:
                log.warning(
                    "soma-publish failed or unavailable — local report at %s", html_path
                )
    except Exception:
        log.exception("EOD report generation failed")


if __name__ == "__main__":
    main()
