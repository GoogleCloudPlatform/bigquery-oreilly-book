
CREATE OR REPLACE FUNCTION blogs.add_fake_user(user_id int64, corp_id STRING) RETURNS STRING
REMOTE WITH CONNECTION `vivid-tuner-338922.us.gcf-conn` -- change this to reflect your PROJECT ID
OPTIONS (
    -- change this to reflect the Trigger URL of your cloud function (look for the TRIGGER tab)
    endpoint = 'https://us-central1-vivid-tuner-338922.cloudfunctions.net/add_fake_user'
)

SELECT
  start_station_id, end_station_id, 
  blogs.add_fake_user(trip_id, bikeid)
FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
LIMIT 10


