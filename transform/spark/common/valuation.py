
from __future__ import annotations


def drawing_power(units_pledged: float, nav: float, eligible_ltv_pct: float) -> float:
    return units_pledged * nav * (eligible_ltv_pct / 100.0)


def ltv_pct(outstanding: float, collateral_value: float) -> float | None:
    if collateral_value <= 0:
        return None
    return outstanding / collateral_value * 100.0


def utilization_pct(outstanding: float, dp: float) -> float | None:
    if dp <= 0:
        return None
    return outstanding / dp * 100.0


def shortfall(outstanding: float, dp: float) -> float:
    return max(0.0, outstanding - dp)


def is_margin_call(outstanding: float, dp: float) -> bool:
    return outstanding > dp


def npa_bucket(dpd: int) -> str:
    if dpd <= 0:
        return "CURRENT"
    if dpd <= 30:
        return "1-30"
    if dpd <= 60:
        return "31-60"
    if dpd <= 90:
        return "61-90"
    return "90+"


def daily_interest(outstanding: float, annual_rate_pct: float) -> float:
    return outstanding * (annual_rate_pct / 100.0) / 365.0
