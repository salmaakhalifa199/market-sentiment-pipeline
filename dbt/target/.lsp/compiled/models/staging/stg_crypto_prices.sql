-- Staging: Crypto Prices — cleans Silver Delta Lake table


SELECT
    symbol,
    price,
    open,
    high,
    low,
    volume,
    price_change,
    change_pct,
    change_magnitude,
    price_direction,
    is_volatile,
    ingested_at,
    processed_at,
    ROUND(high - low, 6)                        AS price_range,
    ROUND((price - open) / NULLIF(open, 0), 6)  AS intraday_return,
    DATE(ingested_at)                            AS trade_date,
    HOUR(ingested_at)                            AS trade_hour
FROM delta.`/delta/silver/crypto_prices`
WHERE price IS NOT NULL AND price > 0 AND symbol IS NOT NULL