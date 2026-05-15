"""
Binance WebSocket Producer
Streams real-time crypto prices → Kafka topic: crypto.prices
"""

import json
import os
import time
import logging
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("binance_producer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "crypto.prices"
SYMBOLS = os.getenv("CRYPTO_SYMBOLS", "btcusdt,ethusdt,solusdt").split(",")

# Build multi-stream URL: <symbol>@ticker for each symbol
STREAM_NAMES = "/".join([f"{s.lower()}@ticker" for s in SYMBOLS])
WS_URL = f"wss://stream.binance.com:9443/stream?streams={STREAM_NAMES}"


def create_producer() -> KafkaProducer:
    """Create and return a KafkaProducer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        retry_backoff_ms=500,
    )


def parse_ticker(raw: dict) -> dict:
    """
    Extract relevant fields from a Binance ticker stream event.
    Full schema: https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-mini-ticker-stream
    """
    data = raw.get("data", {})
    return {
        "symbol":        data.get("s", "").upper(),   # e.g. BTCUSDT
        "price":         float(data.get("c", 0)),      # current close price
        "open":          float(data.get("o", 0)),
        "high":          float(data.get("h", 0)),
        "low":           float(data.get("l", 0)),
        "volume":        float(data.get("v", 0)),      # base asset volume
        "price_change":  float(data.get("p", 0)),      # absolute price change
        "change_pct":    float(data.get("P", 0)),      # percent change
        "ingested_at":   datetime.now(timezone.utc).isoformat(),
        "source":        "binance_ws",
    }


class BinanceProducer:
    def __init__(self):
        self.producer = create_producer()
        self.ws = None

    def on_message(self, ws, message):
        try:
            raw = json.loads(message)
            record = parse_ticker(raw)

            if not record["symbol"]:
                return

            self.producer.send(
                topic=KAFKA_TOPIC,
                key=record["symbol"],
                value=record,
            )
            logger.info(f"Sent → {KAFKA_TOPIC} | {record['symbol']} = ${record['price']:,.2f} ({record['change_pct']:+.2f}%)")

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.warning(f"WebSocket closed: {close_status_code} — {close_msg}")

    def on_open(self, ws):
        logger.info(f"WebSocket connected. Streaming: {SYMBOLS}")

    def run(self):
        while True:
            try:
                logger.info(f"Connecting to Binance WebSocket: {WS_URL}")
                self.ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )
                self.ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.error(f"Connection failed: {e}. Reconnecting in 5s...")
                time.sleep(5)


if __name__ == "__main__":
    logger.info("Starting Binance WebSocket producer...")
    BinanceProducer().run()
