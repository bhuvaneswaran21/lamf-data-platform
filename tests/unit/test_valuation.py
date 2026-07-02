import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..",
                                "transform", "spark", "common"))
import valuation as v 


def test_drawing_power_equity_cap():
    assert v.drawing_power(100, 200, 50) == 10000.0


def test_ltv_pct():
    assert v.ltv_pct(5000, 10000) == 50.0
    assert v.ltv_pct(5000, 0) is None


def test_shortfall_and_margin_call():
    assert v.shortfall(12000, 10000) == 2000.0
    assert v.shortfall(8000, 10000) == 0.0
    assert v.is_margin_call(12000, 10000) is True
    assert v.is_margin_call(9000, 10000) is False


def test_npa_buckets():
    assert v.npa_bucket(0) == "CURRENT"
    assert v.npa_bucket(15) == "1-30"
    assert v.npa_bucket(45) == "31-60"
    assert v.npa_bucket(75) == "61-90"
    assert v.npa_bucket(120) == "90+"


def test_daily_interest():
    assert round(v.daily_interest(100000, 12.045), 2) == 33.0


def test_margin_call_triggers_after_nav_drop():
    units, ltv, outstanding = 100, 50, 9800
    dp_before = v.drawing_power(units, 200, ltv)      
    dp_after = v.drawing_power(units, 200 * 0.88, ltv)  
    assert not v.is_margin_call(outstanding, dp_before)
    assert v.is_margin_call(outstanding, dp_after)
    assert v.shortfall(outstanding, dp_after) == 1000.0
