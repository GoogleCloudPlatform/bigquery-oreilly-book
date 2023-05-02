CREATE SCHEMA IF NOT EXISTS 
  ch02 OPTIONS(location="eu");

CREATE OR REPLACE TABLE ch02.uk_holidays
(
  holiday_name STRING NOT NULL,
  holiday_type STRING DEFAULT "Bank Holiday",
  holiday_date DATE DEFAULT current_date,
  day_of_week STRING DEFAULT "Monday"
);

CREATE OR REPLACE TABLE ch02.uk_holidays_copy AS
   SELECT * FROM ch02.uk_holidays;

ALTER TABLE ch02.uk_holidays
  RENAME COLUMN day_of_week to weekday;

ALTER TABLE ch02.uk_holidays
  ALTER COLUMN holiday_name DROP NOT NULL;

ALTER TABLE ch02.uk_holidays 
	SET OPTIONS(expiration_timestamp=TIMESTAMP "2024-12-31 00:00:00 UTC"]);
	
TRUNCATE TABLE ch02.uk_holiday;

DROP TABLE ch02.uk_holiday;
