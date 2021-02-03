# Trigger a Cloud Run from a BigQuery event

This shows you how to trigger a Cloud Run container whenever an insert happens into a BigQuery table.



1. In BigQuery web console, create a temporary table:

```
CREATE OR REPLACE TABLE ch04.cloud_run_trigger AS
SELECT
  INSTNM, ADM_RATE_ALL, FIRST_GEN, MD_FAMINC, SAT_AVG
FROM
  ch04.selective_firstgen
ORDER BY
  MD_FAMINC ASC
LIMIT 10
```


2. Grant roles/eventarc.admin to the user or service account you are running as.

3. Run ```bq_cloud_run.sh```

4. Visit Cloud Run web console and verify service has been launched and there are no triggers yet.
Make sure to look at logs and triggers to ensure service has been launched.

5. Insert a new row into the table:

INSERT INTO ch04.cloud_run_trigger 
VALUES('abc', 0.1, 0.1, 12345, 1234.0)


6. Look at Cloud Run web console for the service and see that it has been triggered

