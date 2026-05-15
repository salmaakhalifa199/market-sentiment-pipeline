"""
Spark Structured Streaming — Sentiment (Stocktwits + NewsAPI)
Reads from Kafka topics: market.sentiment, market.news
Writes to Delta Lake Bronze → Silver layers
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp,
    when, round as spark_round, array, lit, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType, ArrayType
)

KAFKA_BROKERS       = "kafka:29092"
SENTIMENT_TOPIC     = "market.sentiment"
NEWS_TOPIC          = "market.news"
BRONZE_SENTIMENT    = "/delta/bronze/sentiment"
SILVER_SENTIMENT    = "/delta/silver/sentiment"
BRONZE_NEWS         = "/delta/bronze/news"
SILVER_NEWS         = "/delta/silver/news"
CHECKPOINT_BASE     = "/delta/checkpoints"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SentimentStream")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.1.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .getOrCreate()
    )


# Schema matching stocktwits_producer.py output
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
    StructField("user_sentiment",       StringType(),  True),  # bullish | bearish | neutral
    StructField("sentiment_pos",        DoubleType(),  True),
    StructField("sentiment_neg",        DoubleType(),  True),
    StructField("sentiment_neu",        DoubleType(),  True),
    StructField("sentiment_compound",   DoubleType(),  True),
    StructField("sentiment_label",      StringType(),  True),
    StructField("sentiment_agreement",  BooleanType(), True),
    StructField("symbols_mentioned",    ArrayType(StringType()), True),
    StructField("data_source",          StringType(),  True),
])

# Schema matching news_producer.py output
NEWS_SCHEMA = StructType([
    StructField("article_id",           StringType(),  True),
    StructField("query",                StringType(),  True),
    StructField("source",               StringType(),  True),
    StructField("author",               StringType(),  True),
    StructField("title",                StringType(),  True),
    StructField("description",          StringType(),  True),
    StructField("url",                  StringType(),  True),
    StructField("published_at",         StringType(),  True),
    StructField("ingested_at",          StringType(),  True),
    StructField("sentiment_pos",        DoubleType(),  True),
    StructField("sentiment_neg",        DoubleType(),  True),
    StructField("sentiment_neu",        DoubleType(),  True),
    StructField("sentiment_compound",   DoubleType(),  True),
    StructField("sentiment_label",      StringType(),  True),
    StructField("data_source",          StringType(),  True),
])


def read_kafka_topic(spark: SparkSession, topic: str):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_stocktwits(raw_df):
    return (
        raw_df
        .select(from_json(col("value").cast("string"), STOCKTWITS_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("created_at",  to_timestamp("created_at"))
        .withColumn("ingested_at", to_timestamp("ingested_at"))
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
    """
    Silver enrichments for Stocktwits:
    - Filter nulls
    - Add combined_sentiment_score: weighted avg of VADER + user label
    - Add influence_score: log-scaled follower count
    - Standardize sentiment_label to bullish/bearish/neutral
    """
    return (
        df
        .filter(col("message_id").isNotNull())
        .filter(col("symbol").isNotNull())

        # Normalize user_sentiment to match VADER labels
        .withColumn("user_sentiment_normalized",
            when(col("user_sentiment") == "bullish", "positive")
            .when(col("user_sentiment") == "bearish", "negative")
            .otherwise("neutral")
        )

        # Combined score: VADER compound weighted 40%, user label 60%
        # User label: bullish=1, bearish=-1, neutral=0
        .withColumn("user_label_score",
            when(col("user_sentiment") == "bullish",  lit(1.0))
            .when(col("user_sentiment") == "bearish",  lit(-1.0))
            .otherwise(lit(0.0))
        )
        .withColumn("combined_sentiment_score",
            spark_round(
                col("sentiment_compound") * 0.4 + col("user_label_score") * 0.6,
                4
            )
        )

        # Final sentiment label from combined score
        .withColumn("final_sentiment",
            when(col("combined_sentiment_score") > 0.1,  "bullish")
            .when(col("combined_sentiment_score") < -0.1, "bearish")
            .otherwise("neutral")
        )

        # Influence: more followers = more weight (capped at 1.0)
        .withColumn("influence_score",
            spark_round(
                when(col("user_followers") > 10000, lit(1.0))
                .when(col("user_followers") > 1000,  lit(0.7))
                .when(col("user_followers") > 100,   lit(0.4))
                .otherwise(lit(0.1)),
                2
            )
        )
    )


def enrich_news_silver(df):
    """
    Silver enrichments for News:
    - Filter nulls
    - Categorize sentiment strength
    - Flag high-impact news (strong sentiment)
    """
    return (
        df
        .filter(col("article_id").isNotNull())
        .filter(col("title").isNotNull())

        # Sentiment strength category
        .withColumn("sentiment_strength",
            when(spark_round(col("sentiment_compound"), 2) >= 0.5,  "strong_positive")
            .when(spark_round(col("sentiment_compound"), 2) >= 0.05, "mild_positive")
            .when(spark_round(col("sentiment_compound"), 2) <= -0.5, "strong_negative")
            .when(spark_round(col("sentiment_compound"), 2) <= -0.05,"mild_negative")
            .otherwise("neutral")
        )

        # High impact flag
        .withColumn("is_high_impact",
            when(
                (col("sentiment_compound") >= 0.5) | (col("sentiment_compound") <= -0.5),
                True
            ).otherwise(False)
        )
    )


def write_stream(df, bronze_path, silver_path, checkpoint_prefix, partition_col=None):
    """Write to both Bronze and Silver Delta Lake."""
    bronze_writer = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{checkpoint_prefix}_bronze")
        .option("mergeSchema", "true")
        .start(bronze_path)
    )

    silver_df = (enrich_stocktwits_silver(df)
                 if "user_sentiment" in [f.name for f in df.schema.fields]
                 else enrich_news_silver(df))

    silver_writer_cfg = (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{checkpoint_prefix}_silver")
        .option("mergeSchema", "true")
    )
    if partition_col:
        silver_writer_cfg = silver_writer_cfg.partitionBy(partition_col)

    silver_writer = silver_writer_cfg.start(silver_path)
    return bronze_writer, silver_writer


if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting sentiment streaming jobs...")

    # Stocktwits stream
    raw_sentiment = read_kafka_topic(spark, SENTIMENT_TOPIC)
    parsed_sentiment = parse_stocktwits(raw_sentiment)
    bronze_s, silver_s = write_stream(
        parsed_sentiment, BRONZE_SENTIMENT, SILVER_SENTIMENT, "sentiment", "symbol"
    )

    # News stream
    raw_news = read_kafka_topic(spark, NEWS_TOPIC)
    parsed_news = parse_news(raw_news)
    bronze_n, silver_n = write_stream(
        parsed_news, BRONZE_NEWS, SILVER_NEWS, "news", None
    )

    print(f"Sentiment: Bronze → {BRONZE_SENTIMENT} | Silver → {SILVER_SENTIMENT}")
    print(f"News:      Bronze → {BRONZE_NEWS} | Silver → {SILVER_NEWS}")

    spark.streams.awaitAnyTermination()
