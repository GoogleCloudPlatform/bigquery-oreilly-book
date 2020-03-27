
-- bq show ch09eu || bq mk --location=EU ch09eu

CREATE OR REPLACE MODEL ch09eu.bicycle_model_linear
OPTIONS(input_label_cols=['duration'], model_type='linear_reg')
AS

SELECT 
  duration
  , start_station_name
  , IF(EXTRACT(dayofweek FROM start_date) BETWEEN 2 and 6, 'weekday', 'weekend') as dayofweek
  , FORMAT('%02d', EXTRACT(HOUR FROM start_date)) AS hourofday
FROM `bigquery-public-data`.london_bicycles.cycle_hire



SELECT * FROM ML.PREDICT(MODEL ch09eu.bicycle_model_linear,(
  SELECT
  'Vauxhall Cross, Vauxhall' AS start_station_name
  , 'weekend' as dayofweek
  , '17' AS hourofday)
)

PROJECT=$(gcloud config get-value project)
BUCKET=${PROJECT}-eu
gsutil mb -l eu gs://${BUCKET}
bq extract -m ch09eu.bicycle_model_linear gs://${BUCKET}/bqml_model_export/bicycle_model_linear


gsutil ls gs://${BUCKET}/bqml_model_export/bicycle_model_linear/
    gs://ai-analytics-solutions-eu/bqml_model_export/bicycle_model_linear/
    gs://ai-analytics-solutions-eu/bqml_model_export/bicycle_model_linear/saved_model.pb
    gs://ai-analytics-solutions-eu/bqml_model_export/bicycle_model_linear/assets/
    gs://ai-analytics-solutions-eu/bqml_model_export/bicycle_model_linear/variables/

./deploy.sh gs://${BUCKET}/bqml_model_export/bicycle_model_linear europe-west1 london_bicycles bqml



input.json
{"start_station_name": "Vauxhall Cross, Vauxhall", "dayofweek": "weekend", "hourofday": "17"}

gcloud ai-platform predict --model london_bicycles --version bqml --json-instances input.json
PREDICTED_LABEL
[1329.178180269723]
