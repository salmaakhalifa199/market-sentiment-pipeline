"""
Spark Structured Streaming — Sentiment (Stocktwits + NewsAPI)
Reads: Kafka topics market.sentiment + market.news
Writes: Delta Lake Bronze + Silver layers
"""

import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    when, round as spark_round, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType, ArrayType
)

KAFKA_BROKERS    = "kafka:29092"
SENTIMENT_TOPIC  = "market.sentiment"
NEWS_TOPIC       = "market.news"
BRONZE_SENTIMENT = "/delta/bronze/sentiment"
SILVER_SENTIMENT = "/delta/silver/sentiment"
BRONZE_NEWS      = "/delta/bronze/news"
SILVER_NEWS      = "/delta/silver/news"
CHECKPOINT_BASE  = "/delta/checkpoints"

STOCKTWITS_SCHEMA = StructType([
    StructField("message_id",           StringType(),  True),
    StructField("symbol",               StringType(),  True),
    StructField("body",                 StringType(),  True),
    StructField("created_at",           StringType(),  True),
    StructField("ingested_at",          StringType(),  True),
    StructField("user_id",              StringType(),  True),
    StructField("username",             StringType(),  True),
    StructField("user_followers",       IntegerType(), True),
    StructField("user_following",       IntegerType(), True),
    StructField("user_ideas",           IntegerType(), True),
    StructField("user_watchlist_count", IntegerType(), True),
    StructField("user_sentiment",       StringType(),  True),
    StructField("sentiment_pos",        DoubleType(),  True),
    StructField("sentiment_neg",        DoubleType(),  True),
    StructField("sentiment_neu",        DoubleType(),  True),
    StructField("sentiment_compound",   DoubleType(),  True),
    StructField("sentiment_label",      StringType(),  True),
    StructField("sentiment_agreement",  BooleanType(), True),
    StructField("symbols_mentioned",    ArrayType(StringType()), True),
    StructField("data_source",          StringType(),  True),
])

NEWS_SCHEMA = StructType([
    StructField("article_id",           StringType(), True),
    StructField("query",                StringType(), True),
    StructField("source",               StringType(), True),
    StructField("author",               StringType(), True),
    StructField("title",                StringType(), True),
    StructField("description",          StringType(), True),
    StructField("url",                  StringType(), True),
    StructField("published_at",         StringType(), True),
    StructField("ingested_at",          StringType(), True),
    StructField("sentiment_pos",        DoubleType(), True),
    StructField("sentiment_neg",        DoubleType(), True),
    StructField("sentiment_neu",        DoubleType(), True),
    StructField("sentiment_compound",   DoubleType(), True),
    StructField("sentiment_label",      StringType(), True),
    StructField("data_source",          StringType(), True),
])


def create_spark_session():
    print("DEBUG: Creating SparkSession for sentiment stream...")
    spark = (
        SparkSession.builder
        .appName("SentimentStream")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"DEBUG: SparkSession ready. Master={spark.sparkContext.master}")
    return spark


def read_kafka(spark, topic):
    print(f"DEBUG: Subscribing to Kafka topic: {topic}")
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("kafka.request.timeout.ms", "60000")
        .load()
    )


def parse_stocktwits(raw_df):
    return (
        raw_df
        .select(from_json(col("value").cast("string"), STOCKTWITS_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("created_at",   to_timestamp("created_at"))
        .withColumn("ingested_at",  to_timestamp("ingested_at"))
        .withColumn("processed_at", current_timestamp())
    )


def parse_news(raw_df):
    return (
        raw_df
        .select(from_json(col("value").cast("string"), NEWS_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("published_at", to_timestamp("published_at"))
        .withColumn("ingested_at",  to_timestamp("ingested_at"))
        .withColumn("processed_at", current_timestamp())
    )


def enrich_stocktwits_silver(df):
    return (
        df
        .filter(col("message_id").isNotNull())
        .filter(col("symbol").isNotNull())
        .withColumn("user_label_score",
            when(col("user_sentiment") == "bullish",  lit(1.0))
            .when(col("user_sentiment") == "bearish", lit(-1.0))
            .otherwise(lit(0.0))
        )
        .withColumn("combined_sentiment_score",
            spark_round(col("sentiment_compound") * 0.4 + col("user_label_score") * 0.6, 4)
        )
        .withColumn("final_sentiment",
            when(col("combined_sentiment_score") > 0.1,  lit("bullish"))
            .when(col("combined_sentiment_score") < -0.1, lit("bearish"))
            .otherwise(lit("neutral"))
        )
        .withColumn("influence_score",
            spark_round(
                when(col("user_followers") > 10000, lit(1.0))
                .when(col("user_followers") > 1000,  lit(0.7))
                .when(col("user_followers") > 100,   lit(0.4))
                .otherwise(lit(0.1)),
            2)
        )
    )


def enrich_news_silver(df):
    return (
        df
        .filter(col("article_id").isNotNull())
        .filter(col("title").isNotNull())
        .withColumn("sentiment_strength",
            when(col("sentiment_compound") >= 0.5,  lit("strong_positive"))
            .when(col("sentiment_compound") >= 0.05, lit("mild_positive"))
            .when(col("sentiment_compound") <= -0.5, lit("strong_negative"))
            .when(col("sentiment_compound") <= -0.05, lit("mild_negative"))
            .otherwise(lit("neutral"))
        )
        .withColumn("is_high_impact",
            when((col("sentiment_compound") >= 0.5) | (col("sentiment_compound") <= -0.5), lit(True))
            .otherwise(lit(False))
        )
    )


def debug_batch(df, epoch_id, label):
    count = df.count()
    print(f"DEBUG: [{label}] Batch {epoch_id} — {count} records")
    if count > 0:
        df.show(2, truncate=True)


def make_writer(df, bronze_path, silver_path, checkpoint_prefix, enrich_fn, partition_col=None):
    """Create bronze + silver writeStream for a given parsed DataFrame."""

    def write_both(batch_df, epoch_id):
        # Bronze: raw
        debug_batch(batch_df, epoch_id, f"BRONZE/{checkpoint_prefix}")
        (batch_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(bronze_path))

        # Silver: enriched
        silver_df = enrich_fn(batch_df)
        debug_batch(silver_df, epoch_id, f"SILVER/{checkpoint_prefix}")
        writer = (silver_df.write.format("delta")
                    .mode("append")
                    .option("mergeSchema", "true"))
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.save(silver_path)

    return (
        df.writeStream
        .foreachBatch(write_both)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{checkpoint_prefix}")
        .trigger(processingTime="10 seconds")
        .start()
    )


if __name__ == "__main__":
    print("=" * 60)
    print("STARTING: stream_sentiment.py")
    print("=" * 60)

    try:
        spark = create_spark_session()

        # Stocktwits stream
        raw_sentiment    = read_kafka(spark, SENTIMENT_TOPIC)
        parsed_sentiment = parse_stocktwits(raw_sentiment)
        q_sentiment = make_writer(
            parsed_sentiment,
            BRONZE_SENTIMENT, SILVER_SENTIMENT,
            "sentiment", enrich_stocktwits_silver, "symbol"
        )
        print(f"DEBUG: Sentiment stream started → {BRONZE_SENTIMENT}")

        # News stream
        raw_news    = read_kafka(spark, NEWS_TOPIC)
        parsed_news = parse_news(raw_news)
        q_news = make_writer(
            parsed_news,
            BRONZE_NEWS, SILVER_NEWS,
            "news", enrich_news_silver
        )
        print(f"DEBUG: News stream started → {BRONZE_NEWS}")

        print("DEBUG: Awaiting termination...")
        spark.streams.awaitAnyTermination()

    except Exception as e:
        print(f"FATAL ERROR in stream_sentiment.py: {e}")
        traceback.print_exc()
        sys.exit(1)
