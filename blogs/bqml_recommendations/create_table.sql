CREATE TEMPORARY FUNCTION CLIP_LESS(x FLOAT64, a FLOAT64) AS (
  IF (x < a, a, x)
);
CREATE TEMPORARY FUNCTION CLIP_GT(x FLOAT64, b FLOAT64) AS (
  IF (x > b, b, x)
);
CREATE TEMPORARY FUNCTION CLIP(x FLOAT64, a FLOAT64, b FLOAT64) AS (
  CLIP_GT(CLIP_LESS(x, a), b)
);

CREATE OR REPLACE TABLE advdata.ga360_recommendations_data
AS
WITH CTE_visitor_page_content AS (
    SELECT
        # Schema: https://support.google.com/analytics/answer/3437719?hl=en
        # For a completely unique visit-session ID, we combine combination of fullVisitorId and visitNumber:
        CONCAT(fullVisitorID,'-',CAST(visitNumber AS STRING)) AS visitorId,
        (SELECT MAX(IF(index=10, value, NULL)) FROM UNNEST(hits.customDimensions)) AS latestContentId,
        (LEAD(hits.time, 1) OVER (PARTITION BY fullVisitorId ORDER BY hits.time ASC) - hits.time) AS session_duration
    FROM
        `cloud-training-demos.GA360_test.ga_sessions_sample`,
        UNNEST(hits) AS hits
    WHERE
        # only include hits on pages
        hits.type = "PAGE"
GROUP BY
        fullVisitorId,
        visitNumber,
        latestContentId,
        hits.time ),
aggregate_web_stats AS (
-- Aggregate web stats
SELECT
    visitorId,
    latestContentId as contentId,
    SUM(session_duration) AS session_duration
FROM
    CTE_visitor_page_content
WHERE
    latestContentId IS NOT NULL
GROUP BY
    visitorId,
    latestContentId
HAVING
    session_duration > 0
),
normalized_session_duration AS (
    SELECT APPROX_QUANTILES(session_duration,100)[OFFSET(50)] AS median_duration
    FROM aggregate_web_stats
)
SELECT
   * EXCEPT(session_duration, median_duration),
   CLIP(0.3 * session_duration / median_duration, 0, 1.0) AS normalized_session_duration
FROM
   aggregate_web_stats, normalized_session_duration
