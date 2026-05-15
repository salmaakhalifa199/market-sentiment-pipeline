"""
News API Producer
Fetches financial headlines every 15 minutes → Kafka topic: market.news
Includes VADER sentiment scoring before publishing.
"""

import json
import os
import time
import logging
from datetime import datetime, timezone, timedelta

import requests
from kafka import KafkaProducer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("news_producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "market.news"
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
POLL_INTERVAL_SEC = 900  # 15 minutes

NEWS_QUERIES = [
    "bitcoin cryptocurrency",
    "ethereum crypto",
    "stock market Wall Street",
    "NASDAQ S&P 500",
    "Tesla Apple Nvidia earnings",
]

NEWS_API_URL = "https://newsapi.org/v2/everything"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        retry_backoff_ms=500,
        request_timeout_ms=5000,
        connections_max_idle_ms=540000,
    )


def classify_sentiment(compound: float) -> str:
    if compound >= 0.05:
        return "positive"
    elif compound <= -0.05:
        return "negative"
    return "neutral"


def fetch_articles(query: str, from_dt: datetime) -> list[dict]:
    """Call NewsAPI and return raw articles for a query."""
    params = {
        "q":        query,
        "from":     from_dt.strftime("%Y-%m-%d"),  # Date only, no time (API limitation)
        "sortBy":   "publishedAt",
        "language": "en",
        "pageSize": 20,
        "apiKey":   NEWS_API_KEY,
    }
    try:
        response = requests.get(NEWS_API_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        articles = data.get("articles", [])
        logger.debug(f"NewsAPI query '{query}': got {len(articles)} articles")
        if data.get("status") != "ok":
            logger.warning(f"NewsAPI status not OK: {data.get('status')} - {data.get('message', 'unknown error')}")
        return articles
    except requests.exceptions.HTTPError as e:
        if response.status_code == 401:
            logger.error(f"NewsAPI 401 Unauthorized: Invalid or expired API key")
        elif response.status_code == 429:
            logger.error(f"NewsAPI 429 Too Many Requests: Rate limited")
        else:
            logger.error(f"NewsAPI HTTP error {response.status_code}: {response.text}")
        return []
    except requests.RequestException as e:
        logger.error(f"NewsAPI request failed for '{query}': {e}")
        return []


def build_record(article: dict, query: str, scores: dict) -> dict:
    text = f"{article.get('title', '')} {article.get('description', '')}"
    return {
        "article_id":         hash(article.get("url", "")),
        "query":              query,
        "source":             article.get("source", {}).get("name", "unknown"),
        "author":             article.get("author", ""),
        "title":              article.get("title", ""),
        "description":        article.get("description", ""),
        "url":                article.get("url", ""),
        "published_at":       article.get("publishedAt", ""),
        "ingested_at":        datetime.now(timezone.utc).isoformat(),
        "sentiment_pos":      round(scores["pos"], 4),
        "sentiment_neg":      round(scores["neg"], 4),
        "sentiment_neu":      round(scores["neu"], 4),
        "sentiment_compound": round(scores["compound"], 4),
        "sentiment_label":    classify_sentiment(scores["compound"]),
        "data_source":        "newsapi",
    }


class NewsProducer:
    def __init__(self):
        max_retries = 5
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                self.producer = create_producer()
                logger.info("✓ Kafka connection established")
                break
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Kafka connection failed (attempt {attempt+1}/{max_retries}): {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to connect to Kafka after {max_retries} attempts")
                    raise
        
        self.analyzer = SentimentIntensityAnalyzer()
        self.seen_urls = set()

    def fetch_and_publish(self):
        # Look back 24 hours instead of 20 minutes
        # NewsAPI free tier has a delay - articles appear 6-24 hours after publishing
        from_dt = datetime.now(timezone.utc) - timedelta(hours=24)
        total = 0
        
        logger.info(f"API Key present: {bool(NEWS_API_KEY)}")
        if not NEWS_API_KEY:
            logger.error("NEWS_API_KEY is not set!")
            return

        for query in NEWS_QUERIES:
            articles = fetch_articles(query, from_dt)
            logger.info(f"Query '{query}': {len(articles)} articles found")
            
            for article in articles:
                url = article.get("url", "")
                if url in self.seen_urls:
                    continue

                text = f"{article.get('title', '')} {article.get('description', '')}"
                scores = self.analyzer.polarity_scores(text)
                record = build_record(article, query, scores)

                self.producer.send(
                    topic=KAFKA_TOPIC,
                    key=query[:50],
                    value=record,
                )
                self.seen_urls.add(url)
                total += 1

        # Keep memory bounded
        if len(self.seen_urls) > 10000:
            self.seen_urls = set(list(self.seen_urls)[-10000:])

        logger.info(f"Published {total} new articles → {KAFKA_TOPIC}")

    def run(self):
        logger.info(f"Starting News producer. Polling every {POLL_INTERVAL_SEC // 60} minutes...")
        while True:
            try:
                self.fetch_and_publish()
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    NewsProducer().run()
