
from pyspark.sql import SparkSession

CATALOG = "nessie"
SEED_DIR = "file:///opt/jobs/data/generated"


def spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    for ns in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")
    return spark


def read_csv(spark, name: str):
    return (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{SEED_DIR}/{name}")
    )
