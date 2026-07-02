
import sys
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/jobs/spark")
from common.session import CATALOG, spark_session

S = f"{CATALOG}.silver"
G = f"{CATALOG}.gold"


def table_exists(spark, fqn: str) -> bool:
    ns, tbl = fqn.split(".")[-2], fqn.split(".")[-1]
    return spark.sql(f"SHOW TABLES IN {CATALOG}.{ns}").filter(F.col("tableName") == tbl).count() > 0


def upsert_scd2_customer(spark):
    stg = spark.table(f"{S}.customer").select(
        "customer_id", "pan_token", "kyc_status", "risk_category", "city", "state"
    ).withColumn(
        "_hash", F.sha2(F.concat_ws("|", "kyc_status", "risk_category", "city", "state"), 256)
    )
    target = f"{G}.dim_customer"

    if not table_exists(spark, target):
        (stg.withColumn("scd_valid_from", F.current_date())
            .withColumn("scd_valid_to", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
            .writeTo(target).using("iceberg").create())
        print(f"gold: created {target} (initial load)")
        return

    stg.createOrReplaceTempView("stg_customer")
    spark.sql(f"""
        MERGE INTO {target} t
        USING stg_customer s ON t.customer_id = s.customer_id AND t.is_current = true
        WHEN MATCHED AND t._hash <> s._hash THEN UPDATE SET
            t.is_current = false, t.scd_valid_to = current_date()
    """)
    spark.sql(f"""
        MERGE INTO {target} t
        USING stg_customer s ON t.customer_id = s.customer_id AND t.is_current = true
        WHEN NOT MATCHED THEN INSERT
            (customer_id, pan_token, kyc_status, risk_category, city, state, _hash,
             scd_valid_from, scd_valid_to, is_current)
        VALUES
            (s.customer_id, s.pan_token, s.kyc_status, s.risk_category, s.city, s.state, s._hash,
             current_date(), NULL, true)
    """)
    print(f"gold: SCD2 merged {target}")


def main():
    spark = spark_session("build-gold")

    upsert_scd2_customer(spark)
    spark.table(f"{S}.scheme").writeTo(f"{G}.dim_scheme").using("iceberg").createOrReplace()
    as_of = spark.table(f"{S}.nav").agg(F.max("nav_date").alias("d")).collect()[0]["d"]
    print(f"gold: valuation as-of {as_of}")
    nav = spark.table(f"{S}.nav").filter(F.col("nav_date") == F.lit(as_of)) \
        .select("scheme_code", "nav")

    hold = spark.table(f"{S}.mf_holding").alias("h")
    scheme = spark.table(f"{S}.scheme").select("scheme_code", "eligible_ltv_pct")
    fhd = (
        hold.join(nav, "scheme_code").join(scheme, "scheme_code")
        .withColumn("collateral_value", F.col("units_pledged") * F.col("nav"))
        .withColumn("dp_contrib", F.col("collateral_value") * F.col("eligible_ltv_pct") / 100.0)
        .withColumn("as_of_date", F.lit(as_of))
        .select("customer_id", "scheme_code", "as_of_date", "units_pledged",
                "nav", "collateral_value", "dp_contrib", "lien_status")
    )
    fhd.writeTo(f"{G}.fact_holding_daily").using("iceberg").createOrReplace()

    by_cust = fhd.groupBy("customer_id").agg(
        F.sum("collateral_value").alias("collateral_value"),
        F.sum("dp_contrib").alias("drawing_power"),
    )

    last_pay = (
        spark.table(f"{S}.repayment").filter("status = 'SUCCESS'")
        .groupBy("loan_id").agg(F.max(F.to_timestamp("paid_at")).alias("last_paid"))
    )

    loan = spark.table(f"{S}.loan").alias("l")
    fld = (
        loan.join(by_cust, "customer_id", "left")
        .join(last_pay, "loan_id", "left")
        .withColumn("as_of_date", F.lit(as_of))
        .withColumn("collateral_value", F.coalesce("collateral_value", F.lit(0.0)))
        .withColumn("drawing_power", F.coalesce("drawing_power", F.lit(0.0)))
        .withColumn("ltv_pct", F.when(F.col("collateral_value") > 0,
                    F.col("outstanding") / F.col("collateral_value") * 100).otherwise(None))
        .withColumn("utilization_pct", F.when(F.col("drawing_power") > 0,
                    F.col("outstanding") / F.col("drawing_power") * 100).otherwise(None))
        .withColumn("shortfall_amount",
                    F.greatest(F.lit(0.0), F.col("outstanding") - F.col("drawing_power")))
        .withColumn("margin_call_flag", F.col("outstanding") > F.col("drawing_power"))
        .withColumn("dpd", F.coalesce(
            F.datediff(F.lit(as_of), F.to_date("last_paid")), F.lit(0)))
        .withColumn("npa_bucket", F.expr("""
            CASE WHEN dpd <= 0 THEN 'CURRENT'
                 WHEN dpd <= 30 THEN '1-30'
                 WHEN dpd <= 60 THEN '31-60'
                 WHEN dpd <= 90 THEN '61-90'
                 ELSE '90+' END"""))
        .withColumn("accrued_interest", F.col("outstanding") * F.col("annual_rate") / 100.0 / 365.0)
        .select("loan_id", "customer_id", "as_of_date", "product_type", "outstanding",
                "collateral_value", "drawing_power", "ltv_pct", "utilization_pct",
                "shortfall_amount", "margin_call_flag", "dpd", "npa_bucket",
                "accrued_interest", "annual_rate")
    )
    fld.writeTo(f"{G}.fact_loan_daily").using("iceberg").createOrReplace()

    c360 = (
        fld.groupBy("customer_id").agg(
            F.count("*").alias("active_loans"),
            F.sum("outstanding").alias("total_outstanding"),
            F.sum("collateral_value").alias("total_collateral"),
            F.max("dpd").alias("current_dpd"),
            F.max(F.col("margin_call_flag").cast("int")).alias("in_margin_call"),
        )
        .withColumn("weighted_ltv", F.when(F.col("total_collateral") > 0,
                    F.col("total_outstanding") / F.col("total_collateral") * 100).otherwise(None))
        .withColumn("segment", F.expr("""
            CASE WHEN current_dpd > 90 OR in_margin_call = 1 THEN 'STRESSED'
                 WHEN current_dpd > 0 OR weighted_ltv > 45 THEN 'WATCH'
                 ELSE 'PRIME' END"""))
    )
    c360.writeTo(f"{G}.customer_360").using("iceberg").createOrReplace()


    fld.groupBy("as_of_date").agg(
        F.sum("outstanding").alias("loan_book"),
        F.sum("collateral_value").alias("aum_pledged"),
        F.sum("drawing_power").alias("total_dp"),
        F.count("*").alias("active_loans"),
        (F.sum("outstanding") / F.sum("collateral_value") * 100).alias("wavg_ltv"),
    ).writeTo(f"{G}.mart_portfolio_daily").using("iceberg").createOrReplace()

    fld.groupBy("as_of_date", "npa_bucket").agg(
        F.count("*").alias("loans"),
        F.sum("outstanding").alias("exposure"),
        F.sum(F.col("margin_call_flag").cast("int")).alias("margin_calls"),
        F.sum("shortfall_amount").alias("total_shortfall"),
    ).writeTo(f"{G}.mart_risk_daily").using("iceberg").createOrReplace()

    for t in ["dim_customer", "dim_scheme", "fact_holding_daily", "fact_loan_daily",
              "customer_360", "mart_portfolio_daily", "mart_risk_daily"]:
        print(f"gold: {G}.{t:<20} = {spark.table(f'{G}.{t}').count():>8} rows")
    spark.stop()


if __name__ == "__main__":
    main()
