-- Mart: Hourly market mood index combining news + sentiment
{{ config(materialized='table') }}

WITH news_hourly AS (
    SELECT
        publish_date,
        publish_hour,
        COUNT(*)                                    AS article_count,
        AVG(sentiment_compound)                     AS avg_news_sentiment,
        SUM(CASE WHEN is_high_impact THEN 1 ELSE 0 END) AS high_impact_count,
        SUM(CASE WHEN sentiment_strength = 'strong_positive' THEN 1 ELSE 0 END) AS strong_positive_news,
        SUM(CASE WHEN sentiment_strength = 'strong_negative' THEN 1 ELSE 0 END) AS strong_negative_news
    FROM {{ ref('stg_news') }}
    GROUP BY publish_date, publish_hour
),

sentiment_hourly AS (
    SELECT
        sentiment_date,
        sentiment_hour,
        COUNT(*)                         AS total_messages,
        AVG(combined_sentiment_score)    AS avg_social_sentiment,
        AVG(influence_score)             AS avg_influence
    FROM {{ ref('stg_sentiment') }}
    GROUP BY sentiment_date, sentiment_hour
)

SELECT
    COALESCE(n.publish_date, s.sentiment_date)    AS date,
    COALESCE(n.publish_hour, s.sentiment_hour)    AS hour,
    n.article_count,
    n.avg_news_sentiment,
    n.high_impact_count,
    n.strong_positive_news,
    n.strong_negative_news,
    s.total_messages                              AS social_message_count,
    s.avg_social_sentiment,
    s.avg_influence,

    -- Combined market mood score: 50% news + 50% social
    ROUND(
        COALESCE(n.avg_news_sentiment, 0) * 0.5
        + COALESCE(s.avg_social_sentiment, 0) * 0.5,
    4) AS market_mood_score,

    -- Mood label
    CASE
        WHEN COALESCE(n.avg_news_sentiment, 0) * 0.5 + COALESCE(s.avg_social_sentiment, 0) * 0.5 > 0.15 THEN 'greed'
        WHEN COALESCE(n.avg_news_sentiment, 0) * 0.5 + COALESCE(s.avg_social_sentiment, 0) * 0.5 < -0.15 THEN 'fear'
        ELSE 'neutral'
    END AS market_mood_label,

    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM news_hourly n
FULL OUTER JOIN sentiment_hourly s
    ON n.publish_date = s.sentiment_date
    AND n.publish_hour = s.sentiment_hour
