-- simple select
SELECT start_station_name, start_date, duration
FROM bigquery-public-data.london_bicycles.cycle_hire
LIMIT 5;

SELECT start_station_name, start_date, duration AS rental_duration
FROM bigquery-public-data.london_bicycles.cycle_hire;

SELECT start_station_name, start_date, round(duration/60)
FROM bigquery-public-data.london_bicycles.cycle_hire;

SELECT start_station_name, start_date, round(duration/60) AS duration_minutes
FROM bigquery-public-data.london_bicycles.cycle_hire;

SELECT start_station_name, start_date, round(duration/60) AS duration_minutes
FROM bigquery-public-data.london_bicycles.cycle_hire
WHERE round(duration/60) > 0.0 AND round(duration/60) < 10.0;

SELECT start_station_name, end_station_name, start_date, round(duration/60) AS duration_minutes
FROM bigquery-public-data.london_bicycles.cycle_hire
WHERE round(duration/60) > 0.0 AND round(duration/60) < 10.0 
AND (start_station_name LIKE '%Covent Garden' OR end_station_name LIKE '%Covent Garden');

WITH covent_garden_trips AS (
   SELECT start_station_name, end_station_name, start_date, 
      round(duration/60) AS duration_minutes
   FROM bigquery-public-data.london_bicycles.cycle_hire
   WHERE start_station_name LIKE '%Covent Garden' 
   OR end_station_name LIKE '%Covent Garden')

SELECT * 
FROM covent_garden_trips
WHERE duration_minutes <= 10;

SELECT * EXCEPT(start_station_id, end_station_id, end_date)
FROM bigquery-public-data.london_bicycles.cycle_hire; 

SELECT start_station_name, end_station_name, start_date,
  round(duration/60) AS duration_minutes
FROM bigquery-public-data.london_bicycles.cycle_hire
WHERE start_station_name LIKE '%Covent Garden' 
OR end_station_name LIKE '%Covent Garden'
ORDER BY start_date DESC, duration_minutes ASC;
 
SELECT c.start_date, h.holiday_date
FROM bigquery-public-data.london_bicycles.cycle_hire c 
JOIN ch02.uk_holiday h
ON DATE(c.start_date) = h.holiday_date
LIMIT 5;

SELECT c.start_date, ROUND(c.duration/60) duration,
   h.holiday_date, h.holiday_name, h.holiday_type
FROM bigquery-public-data.london_bicycles.cycle_hire c
JOIN ch02.uk_holiday h
ON DATE(c.start_date) = h.holiday_date
WHERE h.weekday = 'Monday';

SELECT c.start_date, ROUND(c.duration/60) duration,
   h.holiday_date, h.holiday_name, h.holiday_type
FROM bigquery-public-data.london_bicycles.cycle_hire c
LEFT OUTER JOIN ch02.uk_holiday h
ON DATE(c.start_date) = h.holiday_date;

SELECT c.start_date, ROUND(c.duration_sec/60) duration,
   h.holiday_date, h.holiday_name, h.holiday_type
FROM bigquery-public-data.london_bicycles.cycle_hire c 
RIGHT OUTER JOIN ch02.uk_holiday h
ON DATE(c.start_date) = h.holiday_date;

SELECT c.start_date, ROUND(c.duration_sec/60) duration,
   h.holiday_date, h.holiday_name, h.holiday_type
FROM bigquery-public-data.london_bicycles.cycle_hire c 
FULL OUTER JOIN ch02.uk_holiday h
ON DATE(c.start_date) = h.holiday_date;

SELECT c.start_date, ROUND(c.duration_sec/60) duration, 
   h.holiday_date, h.holiday_name, h.holiday_type
FROM bigquery-public-data.london_bicycles.cycle_hire c 
CROSS JOIN ch02.uk_holiday h;

SELECT c.start_date, ROUND(c.duration_sec/60) duration, 
   h.holiday_date, h.holiday_name, h.holiday_type
FROM bigquery-public-data.london_bicycles.cycle_hire c, 
ch02.uk_holiday h;

SELECT COUNT(*) AS number_trips,
  ROUND(AVG(duration/60)) AS avg_duration,
  MIN(start_date) AS min_date, 
  MAX(end_date) AS max_date
FROM bigquery-public-data.london_bicycles.cycle_hire;

SELECT
  h.holiday_name,
  COUNT(*) AS num_trips,
  ROUND(AVG(duration/60)) AS avg_duration
FROM
  bigquery-public-data.london_bicycles.cycle_hire c
JOIN ch02.uk_holiday h ON DATE(c.start_date) = h.holiday_date
GROUP BY h.holiday_name
ORDER BY num_trips DESC;

SELECT
  h.holiday_name,
  COUNT(*) AS num_trips,
  ROUND(AVG(duration/60)) AS avg_duration
FROM
  bigquery-public-data.london_bicycles.cycle_hire c
JOIN ch02.uk_holiday h ON DATE(c.start_date) = h.holiday_date
GROUP BY h.holiday_name
HAVING num_trips >= 100000
ORDER BY num_trips DESC;

SELECT ANY_VALUE(holiday_name HAVING MIN num_trips) AS lowest_trips,
   ANY_VALUE(holiday_name HAVING MAX num_trips) AS highest_trips
FROM (
   SELECT h.holiday_name, COUNT(*) AS num_trips
   FROM bigquery-public-data.london_bicycles.cycle_hire c
   JOIN ch02.uk_holiday h
   ON DATE(c.start_date) = h.holiday_date
   GROUP BY h.holiday_name);
   
SELECT holiday_name
FROM ch02.uk_holiday
GROUP BY holiday_name;

SELECT DISTINCT holiday_name
FROM ch02.uk_holiday;

SELECT COUNT(DISTINCT holiday_name) AS unique_holidays_count
FROM ch02.uk_holiday;

SELECT DISTINCT holiday_name, holiday_date
FROM ch02.uk_holiday
ORDER BY holiday_name; 

SELECT holiday_name, MIN(holiday_date) AS oldest_date,
   MAX(holiday_date) AS newest_date
FROM ch02.uk_holiday
GROUP BY holiday_name
ORDER BY holiday_name;

SELECT holiday_name, 
   ARRAY_AGG(holiday_date ORDER BY holiday_date) AS holiday_date
FROM ch02.uk_holiday
GROUP BY holiday_name
ORDER BY holiday_name;

SELECT holiday_name, holiday_type, 
   STRUCT(holiday_date, weekday) AS calendar 
FROM ch02.uk_holiday;

SELECT holiday_name, holiday_type,
  ARRAY_AGG(STRUCT(holiday_date, weekday) ORDER BY holiday_date) AS calendar
FROM ch02.uk_holiday
GROUP BY holiday_name, holiday_type
ORDER BY holiday_name;

CREATE TABLE ch02.uk_holiday_arr AS
  SELECT holiday_name, holiday_type,
  	ARRAY_AGG(STRUCT(holiday_date, weekday) ORDER BY holiday_date) AS calendar
  FROM ch02.uk_holiday
  GROUP BY holiday_name, holiday_type
  ORDER BY holiday_name;

SELECT holiday_name, ARRAY_LENGTH(calendar) AS num_dates,
  calendar[OFFSET (0)].holiday_date AS oldest_date,
  calendar[ARRAY_LENGTH(calendar) - 1].holiday_date AS newest_date
FROM ch02.uk_holiday_arr
LIMIT 5;

SELECT h.holiday_name, COUNT(*) AS num_trips,
  ROUND(AVG(duration / 60), 2) AS avg_duration
FROM bigquery-public-data.london_bicycles.cycle_hire c
JOIN ch02.uk_holiday h ON DATE(c.start_date) = h.holiday_date
GROUP BY h.holiday_name
ORDER BY num_trips DESC;

SELECT holiday_name, holiday_type, holiday_date, weekday
FROM ch02.uk_holiday_arr, UNNEST(uk_holiday_arr.calendar);

SELECT h.holiday_name, COUNT(*) AS num_trips,
  ROUND(AVG(duration / 60), 2) AS avg_duration
FROM london_bicycles.cycle_hire c
JOIN ch02.uk_holiday h ON DATE(c.start_date) = 
(SELECT holiday_date
 FROM ch02.uk_holiday_arr, 
 UNNEST(uk_holiday_arr.calendar))
GROUP BY h.holiday_name
ORDER BY num_trips DESC;

WITH tmp_calendar AS (
   SELECT holiday_name, holiday_date
   FROM ch02.uk_holiday_arr, UNNEST(uk_holiday_arr.calendar))
      
SELECT c.holiday_name, COUNT(*) AS num_trips, 
  ROUND(AVG(h.duration / 60), 2) AS avg_duration
FROM bigquery-public-data.london_bicycles.cycle_hire h 
JOIN tmp_calendar c
ON DATE(h.start_date) = c.holiday_date
GROUP BY c.holiday_name
ORDER BY num_trips DESC;


