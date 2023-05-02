-- variables
DECLARE PATTERN STRING DEFAULT '%Hyde%';
DECLARE MIN_TRIPS_THRESH INT64 DEFAULT 100;
DECLARE stations ARRAY<STRING>;

-- find stations of interest
SET stations = (
  SELECT ARRAY_AGG(name)
  FROM bigquery-public-data.london_bicycles.cycle_stations
  WHERE name LIKE PATTERN
);

-- loop through a number of thresholds
WHILE MIN_TRIPS_THRESH < 1000 DO

  SELECT
    start_station_name,
    end_station_name,
    ROUND(AVG(duration),2) AS avg_duration,
    COUNT(duration) AS num_trips
  FROM bigquery-public-data.london_bicycles.cycle_hire, 
     UNNEST(stations) AS station
  WHERE
    start_station_name = station
  GROUP BY start_station_name, end_station_name
  HAVING num_trips > MIN_TRIPS_THRESH
  ORDER BY avg_duration DESC
  LIMIT 5;

  SET MIN_TRIPS_THRESH = MIN_TRIPS_THRESH * 2;
END WHILE