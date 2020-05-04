SELECT
  visitorId,
  ARRAY_AGG(STRUCT(contentId, predicted_normalized_session_duration)
            ORDER BY predicted_normalized_session_duration DESC
            LIMIT 3)
FROM ML.RECOMMEND(MODEL advdata.ga360_recommendations_model)
WHERE predicted_normalized_session_duration < 1
GROUP BY visitorId
