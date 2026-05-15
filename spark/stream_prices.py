"""
Spark Structured Streaming — Crypto Prices
Reads from Kafka topic: crypto.prices
Writes to Delta Lake Bronze → Silver layers
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    when, round as spark_round, abs as spark_abs
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType
)

KAFKA_BROKERS   = "kafka:29092"
KAFKA_TOPIC     = "crypto.prices"
BRONZE_PATH     = "/delta/bronze/crypto_prices"
SILVER_PATH     = "/delta/silver/crypto_prices"
CHECKPOINT_B    = "/delta/checkpoints/bronze_prices"
CHECKPOINT_S    = "/delta/checkpoints/silver_prices"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("CryptoPricesStream")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.1.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .getOrCreate()
    )


# Schema matching binance_producer.py output
PRICE_SCHEMA = StructType([
    StructField("symbol",       StringType(),  True),
    StructField("price",        DoubleType(),  True),
    StructField("open",         DoubleType(),  True),
    StructField("high",         DoubleType(),  True),
    StructField("low",          DoubleType(),  True),
    StructField("volume",       DoubleType(),  True),
    StructField("price_change", DoubleType(),  True),
    StructField("change_pct",   DoubleType(),  True),
    StructField("ingested_at",  StringType(),  True),
    StructField("source",       StringType(),  True),
])


def read_from_kafka(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_prices(raw_df):
    """Parse JSON from Kafka value bytes → typed columns."""
    return (
        raw_df
        .select(from_json(col("value").cast("string"), PRICE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("ingested_at", to_timestamp("ingested_at"))
        .withColumn("processed_at", current_timestamp())
    )


def write_bronze(parsed_df):
    """Write raw parsed stream to Bronze Delta Lake (append only)."""
    return (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_B)
        .option("mergeSchema", "true")
        .start(BRONZE_PATH)
    )


def build_silver(parsed_df):
    """
    Silver layer transformations:
    - Filter out bad/null rows
    - Round price fields to 6 decimal places
    - Add price direction signal
    - Add volatility flag (high-low spread > 2%)
    """
    return (
        parsed_df
        # Drop rows with null prices or unknown symbols
        .filter(col("price").isNotNull() & (col("price") > 0))
        .filter(col("symbol").isNotNull())

        # Round all price fields
        .withColumn("price",        spark_round("price", 6))
        .withColumn("open",         spark_round("open", 6))
        .withColumn("high",         spark_round("high", 6))
        .withColumn("low",          spark_round("low", 6))
        .withColumn("change_pct",   spark_round("change_pct", 4))
        .withColumn("price_change", spark_round("price_change", 6))

        # Price direction: bullish / bearish / neutral
        .withColumn("price_direction",
            when(col("change_pct") > 0.5,  "bullish")
            .when(col("change_pct") < -0.5, "bearish")
            .otherwise("neutral")
        )

        # Volatility flag: (high - low) / open > 2%
        .withColumn("is_volatile",
            when(
                (col("high") - col("low")) / col("open") > 0.02,
                True
            ).otherwise(False)
        )

        # Absolute price change magnitude
        .withColumn("change_magnitude", spark_round(spark_abs("change_pct"), 4))
    )


def write_silver(silver_df):
    """Write enriched stream to Silver Delta Lake."""
    return (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_S)
        .option("mergeSchema", "true")
        .partitionBy("symbol")
        .start(SILVER_PATH)
    )


if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting crypto prices streaming job...")

    raw_df    = read_from_kafka(spark)
    parsed_df = parse_prices(raw_df)
    silver_df = build_silver(parsed_df)

    # Write both layers simultaneously
    bronze_query = write_bronze(parsed_df)
    silver_query = write_silver(silver_df)

    print(f"Bronze stream started → {BRONZE_PATH}")
    print(f"Silver stream started → {SILVER_PATH}")

    # Keep both queries running
    spark.streams.awaitAnyTermination()
