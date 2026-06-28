"""Option chain analytics — Max Pain, PCR, strangle strike selection."""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def _parse_chain(chain_data: dict) -> tuple[dict, float]:
    """
    Extract oc dict and spot from Dhan option chain response.
    Dhan format: chain_data["data"]["data"]["oc"] = {"23000.000000": {"ce": {...}, "pe": {...}}, ...}
                 chain_data["data"]["data"]["last_price"] = spot
    """
    inner = chain_data.get("data", {}).get("data", {})
    oc = inner.get("oc", {})
    spot = inner.get("last_price", 0)
    return oc, spot


def compute_max_pain(chain_data: dict) -> float:
    """
    Calculate the Max Pain strike — the price at which option sellers lose the least.
    Returns the strike price as a float.
    """
    oc, spot = _parse_chain(chain_data)
    if not oc:
        logger.warning("Empty option chain for max pain calculation")
        return spot

    # Parse strikes with non-zero OI
    strikes = []
    for strike_str, values in oc.items():
        strike = float(strike_str)
        ce_oi = values.get("ce", {}).get("oi", 0) or 0
        pe_oi = values.get("pe", {}).get("oi", 0) or 0
        if ce_oi > 0 or pe_oi > 0:
            strikes.append({"strike": strike, "ce_oi": ce_oi, "pe_oi": pe_oi})

    if not strikes:
        return spot

    # For each candidate settlement price, compute total pain to option buyers
    min_pain = float("inf")
    max_pain_strike = spot

    for candidate in strikes:
        test_price = candidate["strike"]
        total_pain = 0

        for s in strikes:
            # CE buyers' pain: if settlement > strike, CE is ITM
            if test_price > s["strike"]:
                total_pain += (test_price - s["strike"]) * s["ce_oi"]
            # PE buyers' pain: if settlement < strike, PE is ITM
            if test_price < s["strike"]:
                total_pain += (s["strike"] - test_price) * s["pe_oi"]

        if total_pain < min_pain:
            min_pain = total_pain
            max_pain_strike = test_price

    logger.debug("Max Pain: %.0f (spot: %.1f, distance: %.0f)", max_pain_strike, spot, abs(spot - max_pain_strike))
    return max_pain_strike


def compute_pcr(chain_data: dict) -> dict:
    """
    Calculate Put-Call Ratio by OI and by volume.
    Returns dict with pcr_oi, pcr_volume, total_ce_oi, total_pe_oi.
    """
    oc, _ = _parse_chain(chain_data)

    total_ce_oi = 0
    total_pe_oi = 0
    total_ce_vol = 0
    total_pe_vol = 0

    for values in oc.values():
        ce = values.get("ce", {})
        pe = values.get("pe", {})
        total_ce_oi += ce.get("oi", 0) or 0
        total_pe_oi += pe.get("oi", 0) or 0
        total_ce_vol += ce.get("volume", 0) or 0
        total_pe_vol += pe.get("volume", 0) or 0

    return {
        "pcr_oi": total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0,
        "pcr_volume": total_pe_vol / total_ce_vol if total_ce_vol > 0 else 0,
        "total_ce_oi": total_ce_oi,
        "total_pe_oi": total_pe_oi,
    }


def get_strangle_strikes(chain_data: dict, spot: float, offset: int = 2) -> dict:
    """
    Find ATM strike and select OTM CE (ATM + offset) and OTM PE (ATM - offset).
    NIFTY strikes are at 50-pt intervals.

    Returns dict with ce_strike, pe_strike, ce_premium, pe_premium, atm_strike.
    """
    oc, chain_spot = _parse_chain(chain_data)
    if spot == 0:
        spot = chain_spot

    if not oc:
        logger.warning("Empty option chain for strangle selection")
        return {"ce_strike": 0, "pe_strike": 0, "ce_premium": 0, "pe_premium": 0, "atm_strike": 0}

    # Get all strikes sorted
    all_strikes = sorted(float(k) for k in oc.keys())

    # Find ATM (nearest to spot)
    atm_strike = min(all_strikes, key=lambda s: abs(s - spot))
    atm_idx = all_strikes.index(atm_strike)

    # CE strike = ATM + offset positions up
    ce_idx = min(atm_idx + offset, len(all_strikes) - 1)
    ce_strike = all_strikes[ce_idx]

    # PE strike = ATM - offset positions down
    pe_idx = max(atm_idx - offset, 0)
    pe_strike = all_strikes[pe_idx]

    # Get premiums
    ce_data = oc.get(f"{ce_strike:.6f}", {}).get("ce", {})
    pe_data = oc.get(f"{pe_strike:.6f}", {}).get("pe", {})

    ce_premium = ce_data.get("last_price", 0) or ce_data.get("ltp", 0) or 0
    pe_premium = pe_data.get("last_price", 0) or pe_data.get("ltp", 0) or 0

    logger.debug("Strangle: CE %.0f (₹%.1f) / PE %.0f (₹%.1f), ATM %.0f, spot %.1f",
                 ce_strike, ce_premium, pe_strike, pe_premium, atm_strike, spot)

    return {
        "ce_strike": ce_strike,
        "pe_strike": pe_strike,
        "ce_premium": ce_premium,
        "pe_premium": pe_premium,
        "atm_strike": atm_strike,
    }


def get_strike_premium(chain_data: dict, strike: float, opt_type: str) -> float:
    """Return the current premium (last_price) for a specific strike + option type.

    opt_type is "ce" or "pe". Returns 0.0 if the strike/side is missing.
    Used for marking open strangle legs to market.
    """
    oc, _ = _parse_chain(chain_data)
    if not oc:
        return 0.0
    side = oc.get(f"{float(strike):.6f}", {}).get(opt_type.lower(), {})
    return side.get("last_price", 0) or side.get("ltp", 0) or 0.0


def compute_iv_rank(current_iv: float, iv_history: list[float]) -> float:
    """
    IV Rank: percentile of current IV relative to historical range (0-100).
    IV Rank = (current - min) / (max - min) * 100
    """
    if not iv_history or len(iv_history) < 2:
        return 50.0

    iv_min = min(iv_history)
    iv_max = max(iv_history)

    if iv_max == iv_min:
        return 50.0

    rank = (current_iv - iv_min) / (iv_max - iv_min) * 100
    return max(0.0, min(100.0, rank))
