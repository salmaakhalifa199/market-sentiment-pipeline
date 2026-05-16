-- Staging: News — cleans Silver Delta Lake table
{{ config(materialized='view') }}

SELECT
    article_id,
    title,
    source,
    query,
    sentiment_compound,
    sentiment_label,
    sentiment_strength,
    is_high_impact,
    published_at,
    ingested_at,
    DATE(published_at)  AS publish_date,
    HOUR(published_at)  AS publish_hour
FROM delta.`/delta/silver/news`
WHERE article_id IS NOT NULL AND title IS NOT NULL
