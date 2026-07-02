
import sys
from pyspark.sql import functions as F, Window

sys.path.insert(0, "/opt/jobs/spark")
from common.session import CATALOG, spark_session  

B = f"{CATALOG}.bronze"
S = f"{CATALOG}.silver"


def latest(df, keys, order_col):
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def main():
    spark = spark_session("build-silver")

    cust = spark.table(f"{B}.customers")
    cust = (
        cust.withColumn("pan_token", F.sha2(F.upper(F.col("pan")), 256))
        .drop("pan", "email", "phone")
        .withColumn("dob", F.to_date("dob"))
    )
    cust.writeTo(f"{S}.customer").using("iceberg").createOrReplace()


    scheme = spark.table(f"{B}.schemes").withColumn(
        "eligible_ltv_pct", F.col("eligible_ltv_pct").cast("double"))
    scheme.writeTo(f"{S}.scheme").using("iceberg").createOrReplace()


    nav = spark.table(f"{B}.nav_history").withColumn("nav", F.col("nav").cast("double"))
    nav = nav.filter("nav > 0").withColumn("nav_date", F.to_date("nav_date"))
    nav = latest(nav, ["scheme_code", "nav_date"], "published_at")
    nav.writeTo(f"{S}.nav").using("iceberg").createOrReplace()


    hold = spark.table(f"{B}.holdings")
    valid_schemes = scheme.select("scheme_code").distinct()
    hold = (
        hold.withColumn("units_pledged", F.col("units_pledged").cast("double"))
        .filter("units_pledged >= 0")
        .join(F.broadcast(valid_schemes), "scheme_code", "inner")
    )
    hold = latest(hold, ["holding_id"], "as_of_date")
    hold.writeTo(f"{S}.mf_holding").using("iceberg").createOrReplace()


    loan = spark.table(f"{B}.loans")
    loan = (
        loan.withColumn("outstanding", F.col("outstanding").cast("double"))
        .withColumn("sanctioned_limit", F.col("sanctioned_limit").cast("double"))
        .withColumn("annual_rate", F.col("annual_rate").cast("double"))
        .filter("outstanding >= 0")
    )
    loan.writeTo(f"{S}.loan").using("iceberg").createOrReplace()


    spark.table(f"{B}.disbursements").writeTo(f"{S}.disbursement").using("iceberg").createOrReplace()
    spark.table(f"{B}.repayments").writeTo(f"{S}.repayment").using("iceberg").createOrReplace()

    for t in ["customer", "scheme", "nav", "mf_holding", "loan", "disbursement", "repayment"]:
        print(f"silver: {S}.{t:<12} = {spark.table(f'{S}.{t}').count():>8} rows")
    spark.stop()


if __name__ == "__main__":
    main()
