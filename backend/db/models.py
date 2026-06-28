"""
FinAgent Database Models — with Account isolation (Paper / Live / Replay)
"""

import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path
from config import config

logger = logging.getLogger(__name__)

import os
_db_override = os.getenv("FINAGENT_DB_PATH")
if _db_override:
    DB_PATH = Path(__file__).parent.parent.parent / _db_override
else:
    DB_PATH = Path(__file__).parent.parent.parent / config.db_path
DEFAULT_ACCOUNT = "paper"


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create all tables if they don't exist. Runs migration for account_id."""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            label TEXT NOT NULL,
            starting_capital REAL NOT NULL,
            current_capital REAL NOT NULL,
            hard_floor REAL NOT NULL,
            created_at TEXT DEFAULT (datetime('now', 'localtime')),
            is_active INTEGER DEFAULT 1,
            settings TEXT
        );

        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL DEFAULT 'paper',
            position_id TEXT NOT NULL,
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_date TEXT NOT NULL,
            exit_date TEXT,
            entry_price REAL NOT NULL,
            exit_price REAL,
            quantity INTEGER NOT NULL,
            pnl_gross REAL,
            cost REAL,
            slippage REAL,
            pnl_net REAL,
            exit_reason TEXT,
            margin_used REAL,
            metadata TEXT,
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL DEFAULT 'paper',
            strategy TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_price REAL,
            stop_loss REAL,
            target REAL,
            lot_size INTEGER,
            margin_required REAL,
            confidence REAL,
            reasoning TEXT,
            metadata TEXT,
            status TEXT DEFAULT 'PENDING',
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        );

        CREATE TABLE IF NOT EXISTS daily_pnl (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL DEFAULT 'paper',
            date TEXT NOT NULL,
            capital REAL NOT NULL,
            unrealized_pnl REAL DEFAULT 0,
            realized_pnl_today REAL DEFAULT 0,
            cumulative_pnl REAL DEFAULT 0,
            n_open_positions INTEGER DEFAULT 0,
            margin_used REAL DEFAULT 0,
            floor_distance REAL,
            vix REAL,
            nifty_close REAL,
            UNIQUE(account_id, date)
        );

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT NOT NULL DEFAULT 'paper',
            event_type TEXT NOT NULL,
            message TEXT NOT NULL,
            data TEXT,
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        );

        CREATE INDEX IF NOT EXISTS idx_trades_account ON trades(account_id);
        CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy);
        CREATE INDEX IF NOT EXISTS idx_signals_account ON signals(account_id);
        CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status);
        -- idx_daily_pnl_account created after migration
        CREATE INDEX IF NOT EXISTS idx_events_account ON events(account_id);
        CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);

        CREATE TABLE IF NOT EXISTS positions (
            id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL DEFAULT 'paper',
            symbol TEXT NOT NULL,
            strategy TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_date TEXT NOT NULL,
            entry_price REAL NOT NULL,
            quantity INTEGER NOT NULL,
            stop_loss REAL DEFAULT 0,
            target REAL DEFAULT 0,
            margin_required REAL DEFAULT 0,
            current_price REAL DEFAULT 0,
            metadata TEXT DEFAULT '{}',
            status TEXT DEFAULT 'OPEN',
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        );
        CREATE INDEX IF NOT EXISTS idx_positions_account ON positions(account_id);
        CREATE INDEX IF NOT EXISTS idx_positions_status ON positions(status);
    """)

    # Ensure default accounts exist
    for acc_id, acc_type, label in [("paper", "paper", "Paper Trading"), ("live", "live", "Live Trading")]:
        existing = conn.execute("SELECT id FROM accounts WHERE id = ?", (acc_id,)).fetchone()
        if not existing:
            conn.execute(
                "INSERT INTO accounts (id, type, label, starting_capital, current_capital, hard_floor) VALUES (?, ?, ?, ?, ?, ?)",
                (acc_id, acc_type, label, config.risk.starting_capital, config.risk.starting_capital, config.risk.hard_floor)
            )

    conn.commit()

    # Migration: add account_id to existing rows that lack it
    _migrate_account_column(conn)

    conn.close()
    logger.info("Database initialized at %s", DB_PATH)


def _migrate_account_column(conn):
    """Add account_id column to old tables that don't have it yet."""
    for table in ["trades", "signals", "events"]:
        try:
            cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
            if "account_id" not in cols:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN account_id TEXT NOT NULL DEFAULT 'paper'")
                logger.info("Migrated %s: added account_id column", table)
        except Exception as e:
            logger.debug("Migration check for %s: %s", table, e)

    # daily_pnl migration
    try:
        cols = [row[1] for row in conn.execute("PRAGMA table_info(daily_pnl)").fetchall()]
        if "account_id" not in cols:
            conn.execute("ALTER TABLE daily_pnl ADD COLUMN account_id TEXT NOT NULL DEFAULT 'paper'")
            logger.info("Migrated daily_pnl: added account_id column")
    except Exception as e:
        logger.debug("Migration check for daily_pnl: %s", e)

    # Create index after migration ensures column exists
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_pnl_account ON daily_pnl(account_id)")
    except Exception:
        pass

    conn.commit()


# ============================================================
# ACCOUNT OPERATIONS
# ============================================================

def get_accounts() -> list[dict]:
    conn = get_connection()
    rows = conn.execute("SELECT * FROM accounts ORDER BY type, created_at").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_account(account_id: str) -> dict | None:
    conn = get_connection()
    row = conn.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def create_account(account_id: str, acc_type: str, label: str, capital: float, floor: float) -> dict:
    conn = get_connection()
    conn.execute(
        "INSERT INTO accounts (id, type, label, starting_capital, current_capital, hard_floor) VALUES (?, ?, ?, ?, ?, ?)",
        (account_id, acc_type, label, capital, capital, floor)
    )
    conn.commit()
    conn.close()
    return get_account(account_id)


def update_account_capital(account_id: str, capital: float):
    conn = get_connection()
    conn.execute("UPDATE accounts SET current_capital = ? WHERE id = ?", (capital, account_id))
    conn.commit()
    conn.close()


def delete_account(account_id: str):
    """Delete a replay account and all its data."""
    if account_id in ("paper", "live"):
        raise ValueError("Cannot delete paper or live accounts")
    conn = get_connection()
    for table in ["trades", "signals", "daily_pnl", "events", "positions"]:
        conn.execute(f"DELETE FROM {table} WHERE account_id = ?", (account_id,))
    conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
    conn.commit()
    conn.close()


# ============================================================
# DATA OPERATIONS (all account-scoped)
# ============================================================

def save_trade(trade: dict, account_id: str = DEFAULT_ACCOUNT):
    conn = get_connection()
    conn.execute("""
        INSERT INTO trades (account_id, position_id, strategy, symbol, direction, entry_date,
            exit_date, entry_price, exit_price, quantity, pnl_gross, cost,
            slippage, pnl_net, exit_reason, margin_used, metadata)
        VALUES (?, :position_id, :strategy, :symbol, :direction, :entry_date,
            :exit_date, :entry_price, :exit_price, :quantity, :pnl_gross, :cost,
            :slippage, :pnl_net, :exit_reason, :margin_used, :metadata)
    """, (account_id, trade["position_id"], trade["strategy"], trade["symbol"],
          trade["direction"], trade["entry_date"], trade.get("exit_date"),
          trade["entry_price"], trade.get("exit_price"), trade["quantity"],
          trade.get("pnl_gross"), trade.get("cost"), trade.get("slippage"),
          trade.get("pnl_net"), trade.get("exit_reason"), trade.get("margin_used"),
          json.dumps(trade.get("metadata", {}))))
    conn.commit()
    conn.close()


def save_signal(signal: dict, account_id: str = DEFAULT_ACCOUNT):
    conn = get_connection()
    conn.execute("""
        INSERT INTO signals (account_id, strategy, symbol, direction, entry_price, stop_loss,
            target, lot_size, margin_required, confidence, reasoning, metadata, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (account_id, signal["strategy"], signal["symbol"], signal["direction"],
          signal.get("entry_price"), signal.get("stop_loss"), signal.get("target"),
          signal.get("lot_size"), signal.get("margin_required"), signal.get("confidence"),
          signal.get("reasoning"), json.dumps(signal.get("metadata", {})),
          signal.get("status", "PENDING")))
    conn.commit()
    conn.close()


def save_daily_pnl(record: dict, account_id: str = DEFAULT_ACCOUNT):
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO daily_pnl (account_id, date, capital, unrealized_pnl,
            realized_pnl_today, cumulative_pnl, n_open_positions, margin_used,
            floor_distance, vix, nifty_close)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (account_id, record["date"], record["capital"], record.get("unrealized_pnl", 0),
          record.get("realized_pnl_today", 0), record.get("cumulative_pnl", 0),
          record.get("n_open_positions", 0), record.get("margin_used", 0),
          record.get("floor_distance"), record.get("vix"), record.get("nifty_close")))
    conn.commit()
    conn.close()


def log_event(event_type: str, message: str, data: dict = None, account_id: str = DEFAULT_ACCOUNT):
    conn = get_connection()
    conn.execute(
        "INSERT INTO events (account_id, event_type, message, data) VALUES (?, ?, ?, ?)",
        (account_id, event_type, message, json.dumps(data) if data else None)
    )
    conn.commit()
    conn.close()


def get_trades(account_id: str = DEFAULT_ACCOUNT, strategy: str = None, limit: int = 100) -> list[dict]:
    conn = get_connection()
    if strategy:
        rows = conn.execute(
            "SELECT * FROM trades WHERE account_id = ? AND strategy = ? ORDER BY exit_date DESC LIMIT ?",
            (account_id, strategy, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM trades WHERE account_id = ? ORDER BY exit_date DESC LIMIT ?",
            (account_id, limit)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_signals(account_id: str = DEFAULT_ACCOUNT, status: str = None, limit: int = 50) -> list[dict]:
    conn = get_connection()
    if status:
        rows = conn.execute(
            "SELECT * FROM signals WHERE account_id = ? AND status = ? ORDER BY created_at DESC LIMIT ?",
            (account_id, status, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM signals WHERE account_id = ? ORDER BY created_at DESC LIMIT ?",
            (account_id, limit)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_daily_pnl_history(account_id: str = DEFAULT_ACCOUNT, days: int = 90) -> list[dict]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM daily_pnl WHERE account_id = ? ORDER BY date DESC LIMIT ?",
        (account_id, days)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_events(account_id: str = DEFAULT_ACCOUNT, event_type: str = None, limit: int = 100) -> list[dict]:
    conn = get_connection()
    if event_type:
        rows = conn.execute(
            "SELECT * FROM events WHERE account_id = ? AND event_type = ? ORDER BY created_at DESC LIMIT ?",
            (account_id, event_type, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM events WHERE account_id = ? ORDER BY created_at DESC LIMIT ?",
            (account_id, limit)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ============================================================
# POSITION OPERATIONS (open position tracking)
# ============================================================

def save_position(position: dict, account_id: str = DEFAULT_ACCOUNT):
    """Persist an open position to the DB."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO positions (id, account_id, symbol, strategy, direction, entry_date,
            entry_price, quantity, stop_loss, target, margin_required, current_price, metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (position["id"], account_id, position["symbol"], position["strategy"],
          position["direction"], position["entry_date"], position["entry_price"],
          position["quantity"], position.get("stop_loss", 0), position.get("target", 0),
          position.get("margin_required", 0), position.get("current_price", position["entry_price"]),
          json.dumps(position.get("metadata", {}))))
    conn.commit()
    conn.close()


def close_position_db(position_id: str, account_id: str = DEFAULT_ACCOUNT):
    """Mark a position as CLOSED in the DB."""
    conn = get_connection()
    conn.execute(
        "UPDATE positions SET status = 'CLOSED' WHERE id = ? AND account_id = ?",
        (position_id, account_id)
    )
    conn.commit()
    conn.close()


def update_position_price(position_id: str, current_price: float, account_id: str = DEFAULT_ACCOUNT):
    """Update the marked-to-market price of an open position."""
    conn = get_connection()
    conn.execute(
        "UPDATE positions SET current_price = ? WHERE id = ? AND account_id = ?",
        (current_price, position_id, account_id)
    )
    conn.commit()
    conn.close()


def get_open_positions(account_id: str = DEFAULT_ACCOUNT) -> list[dict]:
    """Return all open positions for an account."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM positions WHERE account_id = ? AND status = 'OPEN' ORDER BY created_at DESC",
        (account_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_open_positions_summary(account_id: str = DEFAULT_ACCOUNT) -> dict:
    """Return count and total margin of open positions."""
    conn = get_connection()
    row = conn.execute(
        "SELECT COUNT(*) as count, COALESCE(SUM(margin_required), 0) as total_margin "
        "FROM positions WHERE account_id = ? AND status = 'OPEN'",
        (account_id,)
    ).fetchone()
    conn.close()
    return {"count": row["count"], "total_margin": row["total_margin"]}


def has_open_position(symbol: str, strategy: str, account_id: str = DEFAULT_ACCOUNT) -> bool:
    """Check if there's already an open position for this symbol+strategy."""
    conn = get_connection()
    row = conn.execute(
        "SELECT COUNT(*) as n FROM positions WHERE account_id = ? AND symbol = ? AND strategy = ? AND status = 'OPEN'",
        (account_id, symbol, strategy)
    ).fetchone()
    conn.close()
    return row["n"] > 0


def get_strategy_summary(account_id: str = DEFAULT_ACCOUNT) -> dict:
    conn = get_connection()
    rows = conn.execute("""
        SELECT strategy,
            COUNT(*) as total_trades,
            SUM(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END) as winners,
            SUM(pnl_net) as total_pnl,
            AVG(pnl_net) as avg_pnl,
            MIN(pnl_net) as worst_trade,
            MAX(pnl_net) as best_trade,
            SUM(cost) as total_costs,
            SUM(slippage) as total_slippage
        FROM trades
        WHERE account_id = ? AND exit_date IS NOT NULL
        GROUP BY strategy
    """, (account_id,)).fetchall()
    conn.close()

    result = {}
    for r in rows:
        d = dict(r)
        d["win_rate"] = d["winners"] / d["total_trades"] if d["total_trades"] > 0 else 0
        result[d["strategy"]] = d
    return result
