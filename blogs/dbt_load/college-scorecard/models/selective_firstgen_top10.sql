{{ config(materialized='view') }}   /* overrides the project definition */

SELECT
  INSTNM, ADM_RATE_ALL, FIRST_GEN, MD_FAMINC, SAT_AVG
FROM
  {{ ref('selective_firstgen') }}
ORDER BY
  MD_FAMINC ASC
LIMIT 10
