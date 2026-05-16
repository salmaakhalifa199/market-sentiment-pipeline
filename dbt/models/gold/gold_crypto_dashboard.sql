-- Gold: Final table powering the Streamlit dashboard
-- One row per symbol per day — clean, aggregated, ready to serve
{{ config(materialized='table') }}

SELECT
    p.symbol,
    p.date,
    p.avg_price,
    p.avg_change_pct,
    p.avg_volatility,
    p.dominant_direction,
    p.price_tick_count,

    -- Sentiment signals
    p.avg_sentiment_score,
    p.weighted_sentiment_score,
    p.bullish_ratio,
    p.bearish_ratio,
    p.sentiment_message_count,
    p.sentiment_price_agreement,

    -- Market mood (from news + social combined)
    m.market_mood_score,
    m.market_mood_label,
    m.high_impact_count          AS high_impact_news_count,
    m.article_count              AS news_article_count,

    -- Final composite signal
    CASE
        WHEN p.weighted_sentiment_score > 0.2 AND m.market_mood_score > 0.1 THEN 'strong_buy_signal'
        WHEN p.weighted_sentiment_score > 0.1 OR  m.market_mood_score > 0.05 THEN 'mild_buy_signal'
        WHEN p.weighted_sentiment_score < -0.2 AND m.market_mood_score < -0.1 THEN 'strong_sell_signal'
        WHEN p.weighted_sentiment_score < -0.1 OR  m.market_mood_score < -0.05 THEN 'mild_sell_signal'
        ELSE 'hold_signal'
    END AS composite_signal,

    CURRENT_TIMESTAMP() AS last_updated

FROM {{ ref('mart_price_sentiment_correlation') }} p
LEFT JOIN {{ ref('mart_hourly_market_mood') }} m
    ON p.date = m.date
    AND p.hour = m.hour
