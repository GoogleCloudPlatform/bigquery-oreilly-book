# Trigger a Cloud Run from a BigQuery event

This shows you how to trigger a Cloud Run container whenever an insert happens into a BigQuery table.


1. Run ```bq_cloud_run.sh```

2. In BigQuery web console, create a temporary table:

```
CREATE OR REPLACE TABLE cloud_run_tmp.cloud_run_trigger AS
SELECT 
  state, gender, year, name, number
FROM `bigquery-public-data.usa_names.usa_1910_current` 
LIMIT 10000
```

4. Visit Cloud Run web console and verify service has been launched and there are no triggers yet.
Make sure to look at logs and triggers to ensure service has been launched.

5. Insert a new row into the table:
```
INSERT INTO cloud_run_tmp.cloud_run_trigger
VALUES('OK', 'F', 2021, 'Joe', 3)
```

6. Look at Cloud Run web console for the service and see that it has been triggered

7. Go to the BigQuery Console and see that you now have a table in the cloud_run_tmp2 dataset.
This new table was created by the Cloud Run container.

