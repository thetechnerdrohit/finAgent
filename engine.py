"""
FinAgent Trading Engine — Central orchestrator.
Connects strategies, risk engine, data layer, and persistence.
"""

import logging
from datetime import datetime
from config import config

logger = logging.getLogger(__name__)


class TradingEngine:
    """
    Core engine that:
    1. Fetches data via data layer
    2. Generates signals via strategies
    3. Validates signals via risk engine
    4. Executes trades (paper or live)
    5. Monitors positions
    6. Persists everything to DB
    """

    def __init__(self, account_id: str = None):
        # Data layer
        from data.dhan_client import DhanClient
        from data.equity_data import EquityData
        from data.option_chain import compute_max_pain, compute_pcr, get_strangle_strikes

        # Risk engine
        from risk.cost_model import ZerodhaCosts
        from risk.floor_monitor import FloorMonitor
        from risk.margin_checker import MarginChecker
        from risk.position_manager import PositionManager

        # Strategies
        from strategies.nifty_strangle import NiftyStrangle
        from strategies.equity_mean_reversion import EquityMeanReversion
        from strategies.equity_momentum import EquityMomentum

        # Database
        from backend.db.models import (
            init_db, save_trade, save_signal, save_daily_pnl, log_event,
            get_account, save_position, close_position_db,
            update_account_capital, get_open_positions, has_open_position,
            update_position_price,
        )

        # Initialize DB
        init_db()

        # Account
        if account_id is None:
            from backend.api.accounts_api import get_active_account_id
            account_id = get_active_account_id()
        self.account_id = account_id

        acc = get_account(account_id) or {}
        starting_capital = acc.get("current_capital", config.risk.starting_capital)
        hard_floor = acc.get("hard_floor", config.risk.hard_floor)

        # DB functions for positions
        self._save_position = save_position
        self._close_position_db = close_position_db
        self._update_account_capital = update_account_capital
        self._has_open_position = has_open_position
        self._update_position_price = update_position_price
        self._get_open_positions = get_open_positions

        # Notifications
        from notifications.telegram import TelegramNotifier

        # Initialize components
        self.dhan = DhanClient()
        self.equity_data = EquityData()
        self.costs = ZerodhaCosts()
        self.notifier = TelegramNotifier()
        self.floor_monitor = FloorMonitor(hard_floor, starting_capital)
        self.margin_checker = MarginChecker(starting_capital)
        self.position_manager = PositionManager()

        # Strategies
        self.strategies = {
            "nifty_strangle": NiftyStrangle(),
            "equity_mean_reversion": EquityMeanReversion(),
            "equity_momentum": EquityMomentum(),
        }

        # DB functions (account-scoped wrappers)
        self._save_trade_raw = save_trade
        self._save_signal_raw = save_signal
        self._save_daily_pnl_raw = save_daily_pnl
        self._log_event_raw = log_event

        # State
        self.capital = starting_capital
        self.pending_signals = []

        # Load any positions that are already open in the DB so monitoring,
        # mark-to-market and exit checks survive process restarts.
        self._rehydrate_positions()

        logger.info("Trading engine initialized. Account: %s, Capital: ₹%s, Floor: ₹%s",
                     account_id, f"{self.capital:,.0f}", f"{hard_floor:,.0f}")

    def _rehydrate_positions(self):
        """Load OPEN positions from the DB into the in-memory position manager."""
        import json
        from risk.position_manager import Position

        existing = {p.position_id for p in self.position_manager.get_open_positions()}
        loaded = 0
        for row in self._get_open_positions(self.account_id):
            if row["id"] in existing:
                continue
            meta = row.get("metadata")
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}
            entry_date = row["entry_date"]
            if isinstance(entry_date, str):
                try:
                    entry_date = datetime.fromisoformat(entry_date)
                except Exception:
                    entry_date = datetime.now()
            pos = Position(
                symbol=row["symbol"],
                strategy=row["strategy"],
                entry_date=entry_date,
                entry_price=row["entry_price"],
                quantity=row["quantity"],
                direction=row["direction"],
                stop_loss=row.get("stop_loss", 0) or 0,
                target=row.get("target", 0) or 0,
                current_price=row.get("current_price") or row["entry_price"],
                metadata=meta or {},
            )
            pos.position_id = row["id"]
            self.position_manager.add_position(pos)
            loaded += 1
        if loaded:
            logger.info("Rehydrated %d open position(s) from DB", loaded)

    def _save_trade(self, trade: dict):
        self._save_trade_raw(trade, account_id=self.account_id)

    def _save_signal(self, signal: dict):
        self._save_signal_raw(signal, account_id=self.account_id)

    def _save_daily_pnl(self, record: dict):
        self._save_daily_pnl_raw(record, account_id=self.account_id)

    def _log_event(self, event_type: str, message: str, data: dict = None):
        self._log_event_raw(event_type, message, data, account_id=self.account_id)

    def run_options_scan(self, index: str = "NIFTY"):
        """Scan option chain and generate strangle signals."""
        logger.info("Running options scan for %s", index)

        if self.floor_monitor.is_breached:
            logger.warning("Floor breached — skipping options scan")
            return

        try:
            # Get data
            idx_config = config.nifty if index == "NIFTY" else config.banknifty
            expiries = self.dhan.get_expiry_list(idx_config.security_id, "IDX_I")

            if not expiries:
                logger.warning("No expiries found for %s", index)
                return

            nearest_expiry = expiries[0]
            chain = self.dhan.get_option_chain(idx_config.security_id, "IDX_I", nearest_expiry)

            if not chain:
                logger.warning("Empty option chain for %s", index)
                return

            # Get VIX (last 5 trading days)
            from datetime import timedelta
            vix_from = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
            vix_df = self.equity_data.get_india_vix(from_date=vix_from)
            vix = float(vix_df["Close"].iloc[-1]) if vix_df is not None and len(vix_df) > 0 else 14.0

            # Get spot
            spot = chain.get("data", {}).get("data", {}).get("last_price", 0)

            # Check for existing open positions
            open_positions = [p for p in self.position_manager.get_open_positions()
                              if p.strategy == f"{index.lower()}_strangle"]

            # Generate signals
            strategy = self.strategies.get(f"{index.lower()}_strangle")
            if not strategy:
                logger.warning("Strategy %s_strangle not found", index.lower())
                return

            data = {
                "option_chain": chain,
                "vix": vix,
                "spot": spot,
                "current_date": datetime.now(),
                "open_positions": open_positions,
            }

            signals = strategy.generate_signals(data)

            for signal in signals:
                # Margin check
                if not self.margin_checker.can_trade(signal.margin_required):
                    logger.info("Signal rejected — insufficient margin (need ₹%s, available ₹%s)",
                                f"{signal.margin_required:,.0f}",
                                f"{self.margin_checker.available_margin():,.0f}")
                    self._log_event("SIGNAL_REJECTED", f"{signal.symbol} — margin insufficient",
                                    {"margin_required": signal.margin_required})
                    continue

                self.pending_signals.append(signal)
                self._save_signal({
                    "strategy": signal.strategy,
                    "symbol": signal.symbol,
                    "direction": signal.direction,
                    "entry_price": signal.entry_price,
                    "stop_loss": signal.stop_loss,
                    "target": signal.target,
                    "lot_size": signal.lot_size,
                    "margin_required": signal.margin_required,
                    "confidence": signal.confidence,
                    "reasoning": signal.reasoning,
                    "metadata": signal.metadata,
                    "status": "PENDING",
                })
                logger.info("Signal generated: %s %s @ ₹%.1f, margin ₹%s",
                            signal.direction, signal.symbol, signal.entry_price,
                            f"{signal.margin_required:,.0f}")
                self.notifier.send_signal({
                    "direction": signal.direction, "symbol": signal.symbol,
                    "strategy": signal.strategy, "entry_price": signal.entry_price,
                    "margin_required": signal.margin_required,
                    "confidence": signal.confidence, "reasoning": signal.reasoning,
                })

        except Exception as e:
            logger.error("Options scan failed: %s", e, exc_info=True)
            self._log_event("ERROR", f"Options scan failed: {e}")

    def run_equity_scan(self):
        """Scan equity stocks for RSI and momentum signals."""
        logger.info("Running equity scan")

        if self.floor_monitor.is_breached:
            logger.warning("Floor breached — skipping equity scan")
            return

        try:
            from datetime import timedelta
            eq_from = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
            stock_data = self.equity_data.get_nifty_universe(from_date=eq_from)

            # Mean reversion signals
            mr_strategy = self.strategies["equity_mean_reversion"]
            mr_data = {
                "stock_prices": stock_data,
                "capital": self.capital,
                "current_date": datetime.now(),
            }
            mr_signals = mr_strategy.generate_signals(mr_data)

            for signal in mr_signals:
                self.pending_signals.append(signal)
                self._save_signal({
                    "strategy": signal.strategy,
                    "symbol": signal.symbol,
                    "direction": signal.direction,
                    "entry_price": signal.entry_price,
                    "stop_loss": signal.stop_loss,
                    "target": signal.target,
                    "lot_size": signal.lot_size,
                    "margin_required": signal.margin_required,
                    "confidence": signal.confidence,
                    "reasoning": signal.reasoning,
                    "metadata": signal.metadata,
                    "status": "PENDING",
                })
                self.notifier.send_signal({
                    "direction": signal.direction, "symbol": signal.symbol,
                    "strategy": signal.strategy, "entry_price": signal.entry_price,
                    "margin_required": signal.margin_required,
                    "confidence": signal.confidence, "reasoning": signal.reasoning,
                })

            logger.info("Equity scan: %d mean reversion signals", len(mr_signals))

        except Exception as e:
            logger.error("Equity scan failed: %s", e, exc_info=True)

    def execute_pending_signals(self, strategy_name: str = None):
        """Execute pending signals (paper or live)."""
        from risk.position_manager import Position

        to_execute = [s for s in self.pending_signals
                      if strategy_name is None or s.strategy == strategy_name]

        for signal in to_execute:
            if self.floor_monitor.is_breached:
                logger.warning("Floor breached — not executing %s", signal.symbol)
                break

            if not self.margin_checker.can_trade(signal.margin_required):
                logger.info("Skipping %s — margin insufficient", signal.symbol)
                continue

            # Skip if already holding this symbol+strategy
            if self._has_open_position(signal.symbol, signal.strategy, self.account_id):
                logger.info("Skipping %s — already have open position", signal.symbol)
                self.pending_signals.remove(signal)
                continue

            # Check capital
            if signal.margin_required > self.capital:
                logger.info("Skipping %s — insufficient capital", signal.symbol)
                continue

            # Create position
            direction = "SHORT" if signal.direction == "SELL" else "LONG"
            position = Position(
                symbol=signal.symbol,
                strategy=signal.strategy,
                entry_date=datetime.now(),
                entry_price=signal.entry_price,
                quantity=signal.lot_size,
                direction=direction,
                stop_loss=signal.stop_loss,
                target=signal.target,
                metadata=signal.metadata,
            )

            position_id = self.position_manager.add_position(position)

            # Persist position to DB and deduct capital
            self._save_position({
                "id": position_id,
                "symbol": signal.symbol,
                "strategy": signal.strategy,
                "direction": direction,
                "entry_date": datetime.now().isoformat(),
                "entry_price": signal.entry_price,
                "quantity": signal.lot_size,
                "stop_loss": signal.stop_loss,
                "target": signal.target,
                "margin_required": signal.margin_required,
                "metadata": signal.metadata,
            }, account_id=self.account_id)

            self.capital -= signal.margin_required
            self._update_account_capital(self.account_id, self.capital)
            self.margin_checker.update_capital(self.capital)

            mode = "PAPER" if config.paper_trading else "LIVE"
            logger.info("[%s] Executed: %s %s %s @ ₹%.1f, qty=%d, margin=₹%.0f, capital=₹%.0f",
                        mode, signal.direction, signal.symbol, signal.strategy,
                        signal.entry_price, signal.lot_size, signal.margin_required, self.capital)

            self._log_event("TRADE_OPENED", f"{signal.direction} {signal.symbol} | margin ₹{signal.margin_required:,.0f} | capital ₹{self.capital:,.0f}",
                            {"position_id": position_id, "price": signal.entry_price,
                             "strategy": signal.strategy, "paper": config.paper_trading})

            self.notifier.send_trade({
                "mode": mode, "direction": signal.direction, "symbol": signal.symbol,
                "entry_price": signal.entry_price, "lot_size": signal.lot_size,
                "strategy": signal.strategy,
            })

            self.pending_signals.remove(signal)

    def update_market_prices(self) -> dict:
        """Refresh the live ``current_price`` of every open position.

        Equity positions are marked from the latest daily close (and RSI is
        computed for exit evaluation); strangle legs are marked from the live
        option chain. The refreshed price is persisted to the DB.

        Returns a ``{position_id: current_data}`` map for exit evaluation.
        Fetch failures (e.g. Data API not subscribed) are skipped, never fatal.
        """
        market = {}
        positions = self.position_manager.get_open_positions()
        if not positions:
            return market

        chain_cache = {}
        for pos in positions:
            try:
                if pos.strategy in ("nifty_strangle", "banknifty_strangle"):
                    cd = self._mark_strangle(pos, chain_cache)
                else:
                    cd = self._mark_equity(pos)
                if not cd:
                    continue
                pos.current_price = cd["current_price"]
                self._update_position_price(pos.position_id, pos.current_price, self.account_id)
                market[pos.position_id] = cd
            except Exception as e:
                logger.warning("Price update failed for %s: %s", pos.symbol, e)
        return market

    def _mark_equity(self, pos) -> dict | None:
        """Mark an equity position from the latest daily close + RSI(14)."""
        import ta
        from datetime import timedelta
        frm = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        df = self.equity_data.get_stock_data(pos.symbol, from_date=frm)
        if df is None or len(df) < 15:
            return None
        rsi_series = ta.momentum.RSIIndicator(df["Close"], window=14).rsi()
        return {
            "current_price": float(df["Close"].iloc[-1]),
            "current_rsi": float(rsi_series.iloc[-1]),
            "current_date": datetime.now(),
        }

    def _mark_strangle(self, pos, chain_cache: dict) -> dict | None:
        """Mark a short-strangle position from current CE+PE premiums."""
        from data.option_chain import get_strike_premium
        index = "BANKNIFTY" if pos.strategy.startswith("banknifty") else "NIFTY"
        idx_cfg = config.banknifty if index == "BANKNIFTY" else config.nifty

        chain = chain_cache.get(index)
        if chain is None:
            expiries = self.dhan.get_expiry_list(idx_cfg.security_id, "IDX_I")
            if not expiries:
                return None
            chain = self.dhan.get_option_chain(idx_cfg.security_id, "IDX_I", expiries[0])
            chain_cache[index] = chain
        if not chain:
            return None

        ce_strike = pos.metadata.get("ce_strike")
        pe_strike = pos.metadata.get("pe_strike")
        if ce_strike is None or pe_strike is None:
            return None

        ce_cur = get_strike_premium(chain, float(ce_strike), "ce")
        pe_cur = get_strike_premium(chain, float(pe_strike), "pe")
        combined = (ce_cur or 0) + (pe_cur or 0)
        if combined <= 0:
            return None
        return {
            "current_price": combined,
            "ce_current_premium": ce_cur or 0,
            "pe_current_premium": pe_cur or 0,
            "current_date": datetime.now(),
        }

    def check_exits(self, market: dict = None):
        """Evaluate each strategy's ``should_exit`` against fresh prices and
        close any position that triggers a stop-loss / target / expiry / timeout."""
        if market is None:
            market = self.update_market_prices()

        floor_breached = self.floor_monitor.is_breached
        for pos in list(self.position_manager.get_open_positions()):
            strat = self.strategies.get(pos.strategy)
            if strat is None:
                continue

            current_data = dict(market.get(pos.position_id, {}))
            current_data.setdefault("current_date", datetime.now())
            current_data.setdefault("current_price", pos.current_price)
            current_data["floor_breached"] = floor_breached

            try:
                do_exit, reason = strat.should_exit(pos, current_data)
            except Exception as e:
                logger.warning("should_exit failed for %s: %s", pos.symbol, e)
                continue

            if do_exit:
                exit_price = current_data.get("current_price", pos.current_price)
                logger.info("EXIT signal: %s %s — %s @ ₹%.1f",
                            pos.strategy, pos.symbol, reason, exit_price or 0)
                self._close_position(pos.position_id, exit_price, reason)

    def monitor_mtm(self):
        """Refresh live prices, check the hard floor, then evaluate exits."""
        market = self.update_market_prices()
        unrealized = self.position_manager.get_unrealized_pnl()
        result = self.floor_monitor.check_mtm(unrealized)

        if result["breached"]:
            logger.critical("HARD FLOOR BREACHED! Capital: ₹%s, Projected: ₹%s. Exiting all positions.",
                            f"{result['capital']:,.0f}", f"{result['projected']:,.0f}")
            self._exit_all_positions("FLOOR_BREACH")
            self._log_event("FLOOR_BREACH", f"Capital dropped to ₹{result['projected']:,.0f}",
                            {"capital": result["capital"], "projected": result["projected"]})
            self.notifier.send_floor_warning(result["projected"], result["floor"])
            return

        # Floor is safe — evaluate per-strategy exit rules against fresh prices.
        self.check_exits(market)

    def close_expiring_positions(self):
        """Close all option positions at expiry (marked to current premium)."""
        self.update_market_prices()
        open_positions = self.position_manager.get_open_positions()
        for pos in open_positions:
            if pos.strategy in ("nifty_strangle", "banknifty_strangle"):
                # At expiry, options settle at intrinsic value (or 0 for OTM)
                self._close_position(pos.position_id, pos.current_price, "expiry")

    def _exit_all_positions(self, reason: str):
        """Emergency exit of all positions."""
        for pos in self.position_manager.get_open_positions():
            # Use current price with emergency slippage
            exit_price = pos.current_price
            self._close_position(pos.position_id, exit_price, reason)

    def _close_position(self, position_id: str, exit_price: float, reason: str):
        """Close a position and persist to DB."""
        result = self.position_manager.close_position(position_id, exit_price, reason)
        if result:
            # Calculate costs
            entry_val = result["entry_price"] * result["quantity"]
            exit_val = exit_price * result["quantity"]

            if result["strategy"] in ("nifty_strangle", "banknifty_strangle"):
                cost = self.costs.strangle_round_trip_cost(entry_val / 2, entry_val / 2, exit_val / 2, exit_val / 2)
                slippage = self.costs.estimate_slippage(result["entry_price"], result["quantity"], 4,
                                                        emergency=(reason == "FLOOR_BREACH"))
            else:
                cost = self.costs.delivery_round_trip_cost(entry_val, exit_val)
                slippage = entry_val * 0.001

            # Restore margin + apply P&L
            # position_manager.close_position() returns gross P&L under "pnl"
            pnl_gross = result.get("pnl_gross", result.get("pnl", 0))
            margin_used = entry_val  # approximate margin as position value
            pnl_net = pnl_gross - cost - slippage
            self.capital += margin_used + pnl_net
            self.floor_monitor.update_capital(self.capital)
            self.margin_checker.update_capital(self.capital)

            # Persist: close position in DB + update capital
            self._close_position_db(position_id, self.account_id)
            self._update_account_capital(self.account_id, self.capital)

            trade = {
                "position_id": position_id,
                "strategy": result["strategy"],
                "symbol": result["symbol"],
                "direction": result["direction"],
                "entry_date": result["entry_date"],
                "exit_date": str(datetime.now()),
                "entry_price": result["entry_price"],
                "exit_price": exit_price,
                "quantity": result["quantity"],
                "pnl_gross": pnl_gross,
                "cost": cost,
                "slippage": slippage,
                "pnl_net": pnl_net,
                "exit_reason": reason,
                "margin_used": margin_used,
                "metadata": result.get("metadata", {}),
            }
            self._save_trade(trade)

            logger.info("Position closed: %s %s, P&L net: ₹%s, capital: ₹%s, reason: %s",
                         result["symbol"], result["strategy"], f"{pnl_net:,.0f}", f"{self.capital:,.0f}", reason)

            self.notifier.send_trade_closed(trade)

    def generate_daily_report(self):
        """Generate and persist daily summary."""
        from backend.db.models import get_strategy_summary

        summary = get_strategy_summary()
        open_pos = self.position_manager.get_open_positions()
        unrealized = self.position_manager.get_unrealized_pnl()

        report = {
            "date": str(datetime.now().date()),
            "capital": self.capital,
            "unrealized_pnl": unrealized,
            "realized_pnl_today": 0,  # TODO: calculate from today's trades
            "cumulative_pnl": self.capital - config.risk.starting_capital,
            "n_open_positions": len(open_pos),
            "margin_used": self.position_manager.get_total_margin_used(),
            "floor_distance": self.capital - config.risk.hard_floor,
            "vix": None,
            "nifty_close": None,
        }

        self._save_daily_pnl(report)

        logger.info("Daily report: Capital ₹%s, P&L ₹%s, Open: %d, Floor dist: ₹%s",
                     f"{self.capital:,.0f}",
                     f"{report['cumulative_pnl']:,.0f}",
                     len(open_pos),
                     f"{report['floor_distance']:,.0f}")

        text = (
            f"📊 <b>FinAgent Daily Report</b> — {report['date']}\n"
            f"Mode: {'PAPER' if config.paper_trading else 'LIVE'}\n\n"
            f"💰 Capital: ₹{self.capital:,.0f}\n"
            f"📈 Cumulative P&L: ₹{report['cumulative_pnl']:+,.0f}\n"
            f"📉 Unrealized P&L: ₹{unrealized:+,.0f}\n"
            f"📋 Open positions: {len(open_pos)}\n"
            f"🛡️ Floor distance: ₹{report['floor_distance']:,.0f}"
        )
        self.notifier.send_daily_report(text)

        return report
