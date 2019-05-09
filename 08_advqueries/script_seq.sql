CREATE OR REPLACE TABLE ch08eu.typical_trip AS
  SELECT
      start_station_name
      , end_station_name
      , APPROX_QUANTILES(duration, 10)[OFFSET(5)] AS typical_duration
      , COUNT(*) AS num_trips
  FROM
    `bigquery-public-data`.london_bicycles.cycle_hire
  GROUP BY
    start_station_name, end_station_name  
;

CREATE OR REPLACE TABLE ch08eu.unusual_days AS
  SELECT 
     EXTRACT (DATE FROM start_date) AS trip_date
     , APPROX_QUANTILES(duration / typical_duration, 10)[OFFSET(5)] AS ratio
     , COUNT(*) AS num_trips_on_day
  FROM 
    `bigquery-public-data`.london_bicycles.cycle_hire AS hire
    , ch08eu.typical_trip AS trip
  WHERE
     hire.start_station_name = trip.start_station_name 
     AND hire.end_station_name = trip.end_station_name
     AND num_trips > 10
  GROUP BY trip_date
  HAVING num_trips_on_day > 10
  ORDER BY ratio DESC
;

DROP TABLE ch08eu.typical_trip;

