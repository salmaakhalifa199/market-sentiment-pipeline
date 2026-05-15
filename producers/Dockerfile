FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run all producers in parallel
CMD ["sh", "-c", "python binance_producer.py & python stocktwits_producer.py & python news_producer.py & wait"]
