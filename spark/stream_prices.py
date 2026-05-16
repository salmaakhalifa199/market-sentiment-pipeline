"""
Spark Structured Streaming — Crypto Prices
Reads: Kafka topic crypto.prices
Writes: Delta Lake /delta/bronze/crypto_prices + /delta/silver/crypto_prices
"""

import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    when, round as spark_round, abs as spark_abs, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

KAFKA_BROKERS = "kafka:29092"
KAFKA_TOPIC   = "crypto.prices"
BRONZE_PATH   = "/delta/bronze/crypto_prices"
SILVER_PATH   = "/delta/silver/crypto_prices"
CHECKPOINT_B  = "/delta/checkpoints/bronze_prices"
CHECKPOINT_S  = "/delta/checkpoints/silver_prices"

PRICE_SCHEMA = StructType([
    StructField("symbol",       StringType(), True),
    StructField("price",        DoubleType(), True),
    StructField("open",         DoubleType(), True),
    StructField("high",         DoubleType(), True),
    StructField("low",          DoubleType(), True),
    StructField("volume",       DoubleType(), True),
    StructField("price_change", DoubleType(), True),
    StructField("change_pct",   DoubleType(), True),
    StructField("ingested_at",  StringType(), True),
    StructField("source",       StringType(), True),
])


def create_spark_session():
    print("DEBUG: Creating SparkSession...")
    spark = (
        SparkSession.builder
        .appName("CryptoPricesStream")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"DEBUG: SparkSession created. Version={spark.version}")
    print(f"DEBUG: Master={spark.sparkContext.master}")
    return spark


def read_kafka(spark):
    print(f"DEBUG: Connecting to Kafka at {KAFKA_BROKERS}, topic={KAFKA_TOPIC}")
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")   # read all existing messages first
        .option("failOnDataLoss", "false")
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "60000")
        .load()
    )
    print("DEBUG: Kafka readStream created successfully")
    return df


def parse_prices(raw_df):
    print("DEBUG: Setting up price parsing...")
    return (
        raw_df
        .select(from_json(col("value").cast("string"), PRICE_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("ingested_at",  to_timestamp("ingested_at"))
        .withColumn("processed_at", current_timestamp())
    )


def enrich_silver(df):
    return (
        df
        .filter(col("price").isNotNull() & (col("price") > 0))
        .filter(col("symbol").isNotNull())
        .withColumn("price",        spark_round("price", 6))
        .withColumn("open",         spark_round("open", 6))
        .withColumn("high",         spark_round("high", 6))
        .withColumn("low",          spark_round("low", 6))
        .withColumn("change_pct",   spark_round("change_pct", 4))
        .withColumn("price_direction",
            when(col("change_pct") > 0.5,   lit("bullish"))
            .when(col("change_pct") < -0.5, lit("bearish"))
            .otherwise(lit("neutral"))
        )
        .withColumn("is_volatile",
            when((col("high") - col("low")) / col("open") > 0.02, lit(True))
            .otherwise(lit(False))
        )
        .withColumn("change_magnitude", spark_round(spark_abs("change_pct"), 4))
    )


def debug_batch(df, epoch_id, layer):
    """Called on each micro-batch — prints count for debugging."""
    count = df.count()
    print(f"DEBUG: [{layer}] Batch {epoch_id} — {count} records")
    if count > 0:
        df.show(3, truncate=False)


def write_bronze(parsed_df):
    print(f"DEBUG: Starting Bronze write to {BRONZE_PATH}")
    return (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_B)
        .option("mergeSchema", "true")
        .foreachBatch(lambda df, eid: (debug_batch(df, eid, "BRONZE"), 
                                       df.write.format("delta")
                                         .mode("append")
                                         .option("mergeSchema", "true")
                                         .save(BRONZE_PATH)))
        .trigger(processingTime="10 seconds")
        .start()
    )


def write_silver(silver_df):
    print(f"DEBUG: Starting Silver write to {SILVER_PATH}")
    return (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_S)
        .option("mergeSchema", "true")
        .foreachBatch(lambda df, eid: (debug_batch(df, eid, "SILVER"),
                                       df.write.format("delta")
                                         .mode("append")
                                         .option("mergeSchema", "true")
                                         .partitionBy("symbol")
                                         .save(SILVER_PATH)))
        .trigger(processingTime="10 seconds")
        .start()
    )


if __name__ == "__main__":
    print("=" * 60)
    print("STARTING: stream_prices.py")
    print("=" * 60)

    try:
        spark    = create_spark_session()
        raw_df   = read_kafka(spark)
        parsed   = parse_prices(raw_df)
        silver   = enrich_silver(parsed)

        bq = write_bronze(parsed)
        sq = write_silver(silver)

        print("DEBUG: Both streams started. Awaiting termination...")
        spark.streams.awaitAnyTermination()

    except Exception as e:
        print(f"FATAL ERROR in stream_prices.py: {e}")
        traceback.print_exc()
        sys.exit(1)
