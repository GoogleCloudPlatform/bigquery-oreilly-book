CREATE OR REPLACE FUNCTION blogs.add_fake_user(bike_id INT64, rental_id INT64) RETURNS STRING
REMOTE WITH CONNECTION `ai-analytics-solutions.us.gcf-conn`
OPTIONS (
    endpoint = 'https://us-central1-ai-analytics-solutions.cloudfunctions.net/add-fake-user'
)




SELECT 
  start_station_id, end_station_id, blogs.add_fake_user(bikeid, trip_id)
FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
LIMIT 10

