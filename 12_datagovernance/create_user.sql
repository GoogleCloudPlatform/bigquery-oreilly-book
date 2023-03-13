--
-- Copyright 2023 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

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
