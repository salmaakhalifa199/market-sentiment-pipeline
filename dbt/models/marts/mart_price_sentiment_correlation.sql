-- Mart: Join price + sentiment by symbol + hour
-- KEY analytical model: does sentiment predict price movement?
{{ config(materialized='table') }}

WITH prices AS (
    SELECT
        symbol,
        trade_date,
        trade_hour,
        AVG(price)            AS avg_price,
        MAX(price)            AS max_price,
        MIN(price)            AS min_price,
        AVG(change_pct)       AS avg_change_pct,
        AVG(change_magnitude) AS avg_volatility,
        COUNT(*)              AS price_tick_count,
        -- Most common direction in this hour
        MODE() WITHIN GROUP (ORDER BY price_direction) AS dominant_direction
    FROM {{ ref('stg_crypto_prices') }}
    GROUP BY symbol, trade_date, trade_hour
),

sentiment AS (
    SELECT
        symbol,
        sentiment_date,
        sentiment_hour,
        COUNT(*)                          AS message_count,
        AVG(combined_sentiment_score)     AS avg_sentiment_score,
        AVG(influence_score)              AS avg_influence,
        SUM(CASE WHEN final_sentiment = 'bullish'  THEN 1 ELSE 0 END) AS bullish_count,
        SUM(CASE WHEN final_sentiment = 'bearish'  THEN 1 ELSE 0 END) AS bearish_count,
        SUM(CASE WHEN final_sentiment = 'neutral'  THEN 1 ELSE 0 END) AS neutral_count,
        -- Weighted sentiment: influence_score * combined_sentiment_score
        SUM(combined_sentiment_score * influence_score)
            / NULLIF(SUM(influence_score), 0)     AS weighted_sentiment_score
    FROM {{ ref('stg_sentiment') }}
    GROUP BY symbol, sentiment_date, sentiment_hour
)

SELECT
    p.symbol,
    p.trade_date                                        AS date,
    p.trade_hour                                        AS hour,
    p.avg_price,
    p.avg_change_pct,
    p.avg_volatility,
    p.dominant_direction,
    p.price_tick_count,
    s.message_count                                     AS sentiment_message_count,
    s.avg_sentiment_score,
    s.weighted_sentiment_score,
    s.avg_influence,
    s.bullish_count,
    s.bearish_count,
    s.neutral_count,
    ROUND(s.bullish_count / NULLIF(s.message_count, 0), 4) AS bullish_ratio,
    ROUND(s.bearish_count / NULLIF(s.message_count, 0), 4) AS bearish_ratio,

    -- Key signal: does sentiment match price direction?
    CASE
        WHEN p.dominant_direction = 'bullish' AND s.weighted_sentiment_score > 0 THEN TRUE
        WHEN p.dominant_direction = 'bearish' AND s.weighted_sentiment_score < 0 THEN TRUE
        ELSE FALSE
    END AS sentiment_price_agreement,

    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM prices p
LEFT JOIN sentiment s
    ON p.symbol    = CONCAT(s.symbol, 'USDT')
    AND p.trade_date = s.sentiment_date
    AND p.trade_hour = s.sentiment_hour
