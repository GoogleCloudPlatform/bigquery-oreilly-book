
WITH etl_data AS (
   SELECT
     * EXCEPT(ADM_RATE_ALL, FIRST_GEN, MD_FAMINC, SAT_AVG, MD_EARN_WNE_P10)
     , {{target.schema}}.cleanup_numeric(ADM_RATE_ALL) AS ADM_RATE_ALL
     , {{target.schema}}.cleanup_numeric(FIRST_GEN) AS FIRST_GEN
     , {{target.schema}}.cleanup_numeric(MD_FAMINC) AS MD_FAMINC
     , {{target.schema}}.cleanup_numeric(SAT_AVG) AS SAT_AVG
     , {{target.schema}}.cleanup_numeric(MD_EARN_WNE_P10) AS MD_EARN_WNE_P10
   FROM
     ch04.college_scorecard_gcs
)

SELECT * FROM etl_data