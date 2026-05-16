-- Staging: Stocktwits Sentiment — cleans Silver Delta Lake table


SELECT
    message_id,
    symbol,
    body,
    user_sentiment,
    sentiment_compound,
    sentiment_label,
    combined_sentiment_score,
    final_sentiment,
    influence_score,
    sentiment_agreement,
    user_followers,
    user_ideas,
    created_at,
    processed_at,
    DATE(created_at)   AS sentiment_date,
    HOUR(created_at)   AS sentiment_hour
FROM delta.`/delta/silver/sentiment`
WHERE message_id IS NOT NULL AND symbol IS NOT NULL