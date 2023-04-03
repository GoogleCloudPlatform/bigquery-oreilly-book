-- Copyright 2023 Google, LLC.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- one-way rentals by year, month
SELECT
 EXTRACT(YEAR FROM start_time) AS year,
 EXTRACT(MONTH FROM start_time) AS month,
 COUNT(start_time) AS one_way_rentals
FROM
 `bigquery-public-data.austin_bikeshare.bikeshare_trips`
WHERE
 start_station_name != end_station_name
 and EXTRACT(YEAR FROM start_time) = 2022
GROUP BY year, month
ORDER BY year DESC, month DESC;

-- are there fewer bicycle rentals on rainy days?
WITH bicycle_rentals AS (
  SELECT
    COUNT(start_time) as num_trips,
    EXTRACT(DATE from start_time) as trip_date
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
  GROUP BY trip_date
),

rainy_days AS
(
SELECT
  date,
  (MAX(prcp) > 5) AS rainy
FROM (
  SELECT
    wx.date AS date,
    IF (wx.element = 'PRCP', wx.value/10, NULL) AS prcp
  FROM
    `bigquery-public-data.ghcn_d.ghcnd_2022` AS wx
  WHERE
    wx.id = 'US1TXHYS059'
)
GROUP BY
  date
)

SELECT
  ROUND(AVG(bk.num_trips)) AS num_trips,
  wx.rainy
FROM bicycle_rentals AS bk
JOIN rainy_days AS wx
ON wx.date = bk.trip_date
GROUP BY wx.rainy;


