import json
import sys

sys.path.insert(0, "/opt/jobs/spark")
from common.session import CATALOG, spark_session
from pyspark.sql import functions as F 

SUITES = {
    "silver": [
        (f"{CATALOG}.silver.nav", [
            ("nav_positive", "nav > 0", True),
            ("nav_date_not_null", "nav_date IS NOT NULL", True),
        ]),
        (f"{CATALOG}.silver.customer", [
            ("pii_tokenized", "pan_token IS NOT NULL", True),
            ("kyc_status_valid", "kyc_status IN ('VERIFIED','PENDING','REJECTED')", True),
        ]),
        (f"{CATALOG}.silver.mf_holding", [
            ("units_non_negative", "units_pledged >= 0", True),
            ("lien_status_valid", "lien_status IN ('REQUESTED','MARKED','RELEASED','INVOKED')", False),
        ]),
        (f"{CATALOG}.silver.loan", [
            ("outstanding_non_negative", "outstanding >= 0", True),
        ]),
    ],
    "gold": [
        (f"{CATALOG}.gold.fact_loan_daily", [
            ("ltv_cap_or_flagged", "drawing_power <= collateral_value OR collateral_value = 0", True),
            ("utilization_sane", "utilization_pct IS NULL OR utilization_pct >= 0", True),
            ("npa_bucket_valid", "npa_bucket IN ('CURRENT','1-30','31-60','61-90','90+')", True),
        ]),
        (f"{CATALOG}.gold.customer_360", [
            ("segment_valid", "segment IN ('PRIME','WATCH','STRESSED')", True),
        ]),
    ],
}


def main():
    layer = sys.argv[1] if len(sys.argv) > 1 else "silver"
    spark = spark_session(f"ge-{layer}")
    results, hard_failures = [], 0

    for table, checks in SUITES.get(layer, []):
        df = spark.table(table)
        total = df.count()
        for name, predicate, hard in checks:
            passed = df.filter(f"NOT ({predicate})").count() == 0 if total else True
            results.append({"table": table, "expectation": name,
                            "passed": passed, "hard": hard})
            if hard and not passed:
                hard_failures += 1
            print(f"[{'PASS' if passed else 'FAIL'}] {table} :: {name}")

    score = round(100.0 * sum(r["passed"] for r in results) / max(1, len(results)), 1)
    print(json.dumps({"layer": layer, "dq_score": score,
                      "hard_failures": hard_failures, "results": results}))
    spark.stop()
    if hard_failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
