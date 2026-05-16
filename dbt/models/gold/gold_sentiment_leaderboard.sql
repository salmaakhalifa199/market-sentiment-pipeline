-- Gold: Top influencers and most discussed assets
-- Powers the "trending" section of the dashboard
{{ config(materialized='table') }}

SELECT
    symbol,
    sentiment_date                          AS date,
    COUNT(*)                                AS total_messages,
    AVG(combined_sentiment_score)           AS avg_sentiment,
    AVG(influence_score)                    AS avg_influence,
    MAX(user_followers)                     AS max_followers,
    SUM(CASE WHEN final_sentiment = 'bullish' THEN 1 ELSE 0 END) AS bullish_msgs,
    SUM(CASE WHEN final_sentiment = 'bearish' THEN 1 ELSE 0 END) AS bearish_msgs,
    ROUND(
        SUM(CASE WHEN final_sentiment = 'bullish' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
    4) AS bullish_pct,

    -- Weighted sentiment score for ranking
    SUM(combined_sentiment_score * influence_score)
        / NULLIF(SUM(influence_score), 0) AS weighted_score,

    CURRENT_TIMESTAMP() AS last_updated

FROM {{ ref('stg_sentiment') }}
GROUP BY symbol, sentiment_date
ORDER BY weighted_score DESC
