"""
Stocktwits Producer
Fetches streams for crypto & stock symbols → Kafka topic: market.sentiment
No API key required for public streams.
"""

import json
import os
import time
import logging
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("stocktwits_producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "market.sentiment"
POLL_INTERVAL_SEC = 120

SYMBOLS = [
    "BTC.X",
    "ETH.X",
    "SOL.X",
    "AAPL",
    "TSLA",
    "NVDA",
]

BASE_URL = "https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        retry_backoff_ms=500,
    )


def classify_vader(compound: float) -> str:
    if compound >= 0.05:
        return "positive"
    elif compound <= -0.05:
        return "negative"
    return "neutral"


def fetch_symbol_stream(symbol: str) -> list[dict]:
    url = BASE_URL.format(symbol=symbol)
    try:
        response = requests.get(
            url,
            timeout=15,
            headers={"User-Agent": "market-sentiment-pipeline/1.0"}
        )
        logger.info(f"Stocktwits [{symbol}] status: {response.status_code}")

        if response.status_code == 429:
            logger.warning(f"Rate limited for {symbol}. Waiting 60s...")
            time.sleep(60)
            return []
        if response.status_code == 404:
            logger.warning(f"Symbol {symbol} not found on Stocktwits")
            return []

        response.raise_for_status()
        data = response.json()
        messages = data.get("messages", [])
        logger.info(f"Stocktwits [{symbol}]: got {len(messages)} messages")
        return messages

    except requests.RequestException as e:
        logger.error(f"Stocktwits API error for {symbol}: {e}")
        return []


def extract_entities(entities: dict) -> dict:
    symbols_mentioned = [
        s.get("symbol", "") for s in entities.get("symbols", [])
    ]
    return {"symbols_mentioned": symbols_mentioned}


def build_record(message: dict, symbol: str, vader_scores: dict) -> dict:
    user = message.get("user", {})
    raw_sentiment = message.get("entities", {}).get("sentiment", None)
    user_sentiment = raw_sentiment.get("basic", "neutral").lower() if raw_sentiment else "neutral"
    entities = message.get("entities", {})

    return {
        "message_id":           str(message.get("id", "")),
        "symbol":               symbol,
        "body":                 message.get("body", ""),
        "created_at":           message.get("created_at", ""),
        "ingested_at":          datetime.now(timezone.utc).isoformat(),
        "user_id":              str(user.get("id", "")),
        "username":             user.get("username", ""),
        "user_followers":       int(user.get("followers", 0)),
        "user_following":       int(user.get("following", 0)),
        "user_ideas":           int(user.get("ideas", 0)),
        "user_watchlist_count": int(user.get("watchlist_stocks_count", 0)),
        "user_sentiment":       user_sentiment,
        "sentiment_pos":        round(vader_scores["pos"], 4),
        "sentiment_neg":        round(vader_scores["neg"], 4),
        "sentiment_neu":        round(vader_scores["neu"], 4),
        "sentiment_compound":   round(vader_scores["compound"], 4),
        "sentiment_label":      classify_vader(vader_scores["compound"]),
        "sentiment_agreement":  user_sentiment == classify_vader(vader_scores["compound"]).replace("positive", "bullish").replace("negative", "bearish"),
        "symbols_mentioned":    extract_entities(entities)["symbols_mentioned"],
        "data_source":          "stocktwits",
    }


class StocktwitsProducer:
    def __init__(self):
        max_retries = 5
        for attempt in range(max_retries):
            try:
                self.producer = create_producer()
                logger.info("✓ Kafka connection established")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Kafka not ready, retrying in 5s... ({e})")
                    time.sleep(5)
                else:
                    raise

        self.analyzer = SentimentIntensityAnalyzer()
        self.seen_ids = set()

    def fetch_and_publish(self):
        total = 0
        for symbol in SYMBOLS:
            messages = fetch_symbol_stream(symbol)
            for message in messages:
                msg_id = str(message.get("id", ""))
                if msg_id in self.seen_ids:
                    continue

                body = message.get("body", "")
                vader_scores = self.analyzer.polarity_scores(body)
                record = build_record(message, symbol, vader_scores)

                self.producer.send(
                    topic=KAFKA_TOPIC,
                    key=symbol,
                    value=record,
                )
                self.seen_ids.add(msg_id)
                total += 1

            time.sleep(3)  # gentle rate limiting between symbols

        if len(self.seen_ids) > 10000:
            self.seen_ids = set(list(self.seen_ids)[-10000:])

        logger.info(f"Published {total} new messages → {KAFKA_TOPIC}")
        self.producer.flush()

    def run(self):
        logger.info(f"Starting Stocktwits producer. Polling every {POLL_INTERVAL_SEC}s...")
        while True:
            try:
                self.fetch_and_publish()
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    StocktwitsProducer().run()
