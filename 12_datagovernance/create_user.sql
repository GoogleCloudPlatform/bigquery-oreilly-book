create schema bigquerybook2e.fitdw
  options (
    location = 'us-central1'
  );

create or replace table fitdw.User(
	userid STRING PRIMARY KEY NOT ENFORCED,
	signup_date DATE,
	gender STRING,
	age_group STRING,
	height_in NUMERIC,
	weight_lb NUMERIC,
	city STRING,
	country_code STRING,
	mobile_os STRING,
	premium_status STRING
)
cluster by country_code;
