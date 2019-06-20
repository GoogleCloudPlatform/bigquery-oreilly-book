CREATE OR REPLACE PROCEDURE ch08eu.sp_most_unusual(
   IN MIN_TRIPS_THRESH INT64, 
   OUT result ARRAY<STRUCT<trip_date DATE, ratio FLOAT64, num_trips_on_day INT64>>)

BEGIN
  CREATE TEMPORARY TABLE typical_trip AS
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

  SET result = (
    WITH unusual_trips AS (
      SELECT
         EXTRACT (DATE FROM start_date) AS trip_date
         , APPROX_QUANTILES(duration / typical_duration, 10)[OFFSET(5)] AS ratio
         , COUNT(*) AS num_trips_on_day
      FROM
        `bigquery-public-data`.london_bicycles.cycle_hire AS hire
        , typical_trip AS trip
      WHERE
         hire.start_station_name = trip.start_station_name
         AND hire.end_station_name = trip.end_station_name
         AND num_trips > MIN_TRIPS_THRESH
      GROUP BY trip_date
      HAVING num_trips_on_day > MIN_TRIPS_THRESH
    ) 
    SELECT 
    ARRAY_AGG(STRUCT(trip_date, ratio, num_trips_on_day) ORDER BY ratio DESC LIMIT 3)
    FROM unusual_trips
  );
 
END;
