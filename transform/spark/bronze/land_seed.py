
import sys
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/jobs/spark")
from common.session import CATALOG, spark_session, read_csv  

TABLES = ["customers", "schemes", "nav_history", "holdings",
          "loans", "disbursements", "repayments"]


def main():
    spark = spark_session("bronze-land-seed")
    for name in TABLES:
        df = (
            read_csv(spark, f"{name}.csv")
            .withColumn("_source", F.lit(f"seed/{name}.csv"))
            .withColumn("_ingested_at", F.current_timestamp())
        )
        target = f"{CATALOG}.bronze.{name}"
        df.writeTo(target).using("iceberg").createOrReplace()
        print(f"bronze: wrote {df.count():>8} rows -> {target}")
    spark.stop()


if __name__ == "__main__":
    main()
