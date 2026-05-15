"""
Stocktwits Producer
Fetches streams for crypto & stock symbols → Kafka topic: market.sentiment
Stocktwits users self-label posts as bullish/bearish — ground truth sentiment.
API Docs: https://api.stocktwits.com/developers/docs
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
POLL_INTERVAL_SEC = 120  # every 2 minutes

# Stocktwits uses $ prefix for symbols
SYMBOLS = [
    "BTC.X",   # Bitcoin
    "ETH.X",   # Ethereum
    "SOL.X",   # Solana
    "AAPL",    # Apple
    "TSLA",    # Tesla
    "NVDA",    # Nvidia
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
    """Fetch latest 30 messages for a symbol from Stocktwits."""
    url = BASE_URL.format(symbol=symbol)
    try:
        response = requests.get(url, timeout=10, headers={"User-Agent": "market-sentiment-pipeline/1.0"})
        if response.status_code == 429:
            logger.warning(f"Rate limited for {symbol}. Waiting 60s...")
            time.sleep(60)
            return []
        response.raise_for_status()
        return response.json().get("messages", [])
    except requests.RequestException as e:
        logger.error(f"Stocktwits API error for {symbol}: {e}")
        return []


def extract_entities(entities: dict) -> dict:
    """Extract mentioned symbols and sentiment from entities block."""
    symbols_mentioned = [
        s.get("symbol", "") for s in entities.get("symbols", [])
    ]
    return {"symbols_mentioned": symbols_mentioned}


def build_record(message: dict, symbol: str, vader_scores: dict) -> dict:
    """
    Build a clean Kafka record from a Stocktwits message.
    Key advantage: user_sentiment is GROUND TRUTH (self-labeled by trader).
    """
    user = message.get("user", {})
    # Stocktwits sentiment: {"basic": "Bullish"} or {"basic": "Bearish"} or None
    raw_sentiment = message.get("entities", {}).get("sentiment", None)
    user_sentiment = raw_sentiment.get("basic", "neutral").lower() if raw_sentiment else "neutral"

    entities = message.get("entities", {})

    return {
        # Identifiers
        "message_id":           message.get("id"),
        "symbol":               symbol,

        # Content
        "body":                 message.get("body", ""),
        "created_at":           message.get("created_at", ""),
        "ingested_at":          datetime.now(timezone.utc).isoformat(),

        # User info
        "user_id":              user.get("id"),
        "username":             user.get("username", ""),
        "user_followers":       user.get("followers", 0),
        "user_following":       user.get("following", 0),
        "user_ideas":           user.get("ideas", 0),       # total posts = experience signal
        "user_watchlist_count": user.get("watchlist_stocks_count", 0),

        # Ground truth sentiment (self-labeled by trader) ← KEY SIGNAL
        "user_sentiment":       user_sentiment,             # bullish | bearish | neutral

        # VADER NLP sentiment on message body (second signal)
        "sentiment_pos":        round(vader_scores["pos"], 4),
        "sentiment_neg":        round(vader_scores["neg"], 4),
        "sentiment_neu":        round(vader_scores["neu"], 4),
        "sentiment_compound":   round(vader_scores["compound"], 4),
        "sentiment_label":      classify_vader(vader_scores["compound"]),

        # Sentiment agreement signal
        "sentiment_agreement":  user_sentiment == classify_vader(vader_scores["compound"]).replace("positive", "bullish").replace("negative", "bearish"),

        # Other mentioned symbols
        **extract_entities(entities),

        "data_source":          "stocktwits",
    }


class StocktwitsProducer:
    def __init__(self):
        self.producer = create_producer()
        self.analyzer = SentimentIntensityAnalyzer()
        self.seen_ids = set()

    def fetch_and_publish(self):
        total = 0
        for symbol in SYMBOLS:
            messages = fetch_symbol_stream(symbol)
            for message in messages:
                msg_id = message.get("id")
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

            time.sleep(2)  # gentle rate limiting between symbols

        # Keep memory bounded
        if len(self.seen_ids) > 10000:
            self.seen_ids = set(list(self.seen_ids)[-10000:])

        logger.info(f"Published {total} new messages → {KAFKA_TOPIC}")

    def run(self):
        logger.info(f"Starting Stocktwits producer. Polling every {POLL_INTERVAL_SEC // 60} minutes...")
        while True:
            try:
                self.fetch_and_publish()
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    StocktwitsProducer().run()
