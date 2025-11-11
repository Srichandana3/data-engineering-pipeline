# etl_spark_to_processed.py
# -------------------------
# Kafka → MinIO(raw) → Spark(clean/curate) → MinIO(processed) → Postgres
#
# Run (host):
#   python etl_spark_to_processed.py --endpoint http://localhost:9000
#
# Run (inside a container, e.g., Airflow or Spark master):
#   python etl_spark_to_processed.py --endpoint http://minio:9000
#
# If you prefer spark-submit, you don't need to pass --packages because this
# script sets spark.jars.packages for hadoop-aws already.

import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)
from pyspark.sql.functions import (
    col, from_json, to_timestamp, date_format, when, lit, trim, length
)


def build_spark(endpoint: str, access_key: str, secret_key: str) -> SparkSession:
    """
    Build a SparkSession configured for S3A → MinIO and Postgres JDBC.
    """
    return (
        SparkSession.builder
        .appName("etl-to-processed")
        # S3A + JDBC support jars
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.6.0"
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()
    )


def schema_transactions() -> StructType:
    """Expected schema of raw JSON transactions."""
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("device", StringType(), True),
        StructField("status", StringType(), True),
        StructField("ts", StringType(), True),
        StructField("timestamp_iso", StringType(), True),
    ])


def main():
    ap = argparse.ArgumentParser(description="ETL: MinIO raw → processed parquet → Postgres")
    ap.add_argument("--endpoint", default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
    ap.add_argument("--access-key", default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    ap.add_argument("--secret-key", default=os.getenv("MINIO_SECRET_KEY", "minioadmin"))
    ap.add_argument("--raw-path", default="s3a://raw/streaming/*/*/*/*/*.json")
    ap.add_argument("--out-path", default="s3a://processed/transactions")
    ap.add_argument("--error-path", default="s3a://processed/_errors/transactions")
    args = ap.parse_args()

    spark = build_spark(args.endpoint, args.access_key, args.secret_key)
    spark.sparkContext.setLogLevel("WARN")

    schema = schema_transactions()

    # Read + parse
    raw_text = spark.read.text(args.raw_path)
    parsed = raw_text.select(
        from_json(col("value"), schema).alias("j"),
        col("value").alias("raw_value")
    )

    good = (
        parsed.where(col("j").isNotNull())
        .select("j.*")
        .withColumn("transaction_id", trim(col("transaction_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("currency", trim(col("currency")))
        .withColumn("merchant", trim(col("merchant")))
        .withColumn("device", trim(col("device")))
        .withColumn(
            "event_time",
            when(length(trim(col("timestamp_iso"))) > 0,
                 to_timestamp(col("timestamp_iso")))
            .otherwise(to_timestamp(col("ts")))
        )
        .where(col("transaction_id").isNotNull() & (length(col("transaction_id")) > 0))
        .where(col("amount").isNotNull() & (col("amount") > 0.0))
        .withColumn("dt", date_format(col("event_time"), "yyyy-MM-dd"))
        .withColumn("hr", date_format(col("event_time"), "HH"))
    )

    # Deduplicate
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc
    w = Window.partitionBy("transaction_id").orderBy(desc("event_time"))
    good_dedup = good.withColumn("rn", row_number().over(w)).where(col("rn") == 1).drop("rn")

    # Handle bad data
    bad = parsed.where(col("j").isNull()).select("raw_value")
    if bad.head(1):
        bad.write.mode("append").text(args.error_path)

    # =========================
    # Write curated to MinIO
    # =========================
    (
        good_dedup.write
        .mode("overwrite")
        .partitionBy("dt", "device")
        .parquet(args.out_path)
    )
    print(f"✅ Wrote curated Parquet to {args.out_path}")

    # =========================
    # Write curated to Postgres
    # =========================
    print("✅ Writing curated data to Postgres table: transactions_curated")

    good_dedup.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/analytics") \
        .option("dbtable", "transactions_curated") \
        .option("user", "postgres") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("✅ Data successfully written to Postgres (table: transactions_curated)")

    spark.stop()


if __name__ == "__main__":
    main()
