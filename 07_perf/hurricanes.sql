


CREATE OR REPLACE TABLE ch07.hurricanes_raw AS (

SELECT sid, season, number, basin, name, iso_time, nature, usa_sshs,
       usa_latitude, usa_longitude, usa_wind, usa_pressure, 
       tokyo_latitude, tokyo_longitude, tokyo_wind, tokyo_pressure,
       cma_latitude, cma_longitude, cma_wind, cma_pressure,
       hko_latitude, hko_longitude, hko_wind, hko_pressure,
       newdelhi_latitude, newdelhi_longitude, newdelhi_wind, newdelhi_pressure,
       reunion_latitude, reunion_longitude, reunion_wind, reunion_pressure,
       bom_latitude, bom_longitude, bom_wind, bom_pressure,
       wellington_latitude, wellington_longitude, wellington_wind, wellington_pressure,
       nadi_latitude, nadi_longitude, nadi_wind, nadi_pressure
FROM `bigquery-public-data`.noaa_hurricanes.hurricanes

)


CREATE OR REPLACE TABLE ch07.hurricanes_nested AS (

SELECT sid, season, number, basin, name, iso_time, nature, usa_sshs,
       STRUCT(usa_latitude AS latitude, usa_longitude AS longitude, usa_wind AS wind, usa_pressure AS pressure) AS usa,
       STRUCT(tokyo_latitude AS latitude, tokyo_longitude AS longitude, tokyo_wind AS wind, tokyo_pressure AS pressure) AS tokyo,
       STRUCT(cma_latitude AS latitude, cma_longitude AS longitude, cma_wind AS wind, cma_pressure AS pressure) AS cma,
       STRUCT(hko_latitude AS latitude, hko_longitude AS longitude, hko_wind AS wind, hko_pressure AS pressure) AS hko,
       STRUCT(newdelhi_latitude AS latitude, newdelhi_longitude AS longitude, newdelhi_wind AS wind, newdelhi_pressure AS pressure) AS newdelhi,
       STRUCT(reunion_latitude AS latitude, reunion_longitude AS longitude, reunion_wind AS wind, reunion_pressure AS pressure) AS reunion,
       STRUCT(bom_latitude AS latitude, bom_longitude AS longitude, bom_wind AS wind, bom_pressure AS pressure) AS bom,
       STRUCT(wellington_latitude AS latitude, wellington_longitude AS longitude, wellington_wind AS wind, wellington_pressure AS pressure) AS wellington,
       STRUCT(nadi_latitude AS latitude, nadi_longitude AS longitude, nadi_wind AS wind, nadi_pressure AS pressure) AS nadi
FROM `bigquery-public-data`.noaa_hurricanes.hurricanes

)


CREATE OR REPLACE TABLE ch07.hurricanes_nested_obs AS (

SELECT sid, season, number, basin, name, iso_time, nature, usa_sshs,
   STRUCT(
       STRUCT(usa_latitude AS latitude, usa_longitude AS longitude, usa_wind AS wind, usa_pressure AS pressure) AS usa,
       STRUCT(tokyo_latitude AS latitude, tokyo_longitude AS longitude, tokyo_wind AS wind, tokyo_pressure AS pressure) AS tokyo,
       STRUCT(cma_latitude AS latitude, cma_longitude AS longitude, cma_wind AS wind, cma_pressure AS pressure) AS cma,
       STRUCT(hko_latitude AS latitude, hko_longitude AS longitude, hko_wind AS wind, hko_pressure AS pressure) AS hko,
       STRUCT(newdelhi_latitude AS latitude, newdelhi_longitude AS longitude, newdelhi_wind AS wind, newdelhi_pressure AS pressure) AS newdelhi,
       STRUCT(reunion_latitude AS latitude, reunion_longitude AS longitude, reunion_wind AS wind, reunion_pressure AS pressure) AS reunion,
       STRUCT(bom_latitude AS latitude, bom_longitude AS longitude, bom_wind AS wind, bom_pressure AS pressure) AS bom,
       STRUCT(wellington_latitude AS latitude, wellington_longitude AS longitude, wellington_wind AS wind, wellington_pressure AS pressure) AS wellington,
       STRUCT(nadi_latitude AS latitude, nadi_longitude AS longitude, nadi_wind AS wind, nadi_pressure AS pressure) AS nadi
   ) AS obs
FROM `bigquery-public-data`.noaa_hurricanes.hurricanes

)



CREATE OR REPLACE TABLE ch07.hurricanes_nested_track AS (

SELECT sid, season, number, basin, name,
 ARRAY_AGG(
   STRUCT(
       iso_time,
       nature,
       usa_sshs,
       STRUCT(usa_latitude AS latitude, usa_longitude AS longitude, usa_wind AS wind, usa_pressure AS pressure) AS usa,
       STRUCT(tokyo_latitude AS latitude, tokyo_longitude AS longitude, tokyo_wind AS wind, tokyo_pressure AS pressure) AS tokyo,
       STRUCT(cma_latitude AS latitude, cma_longitude AS longitude, cma_wind AS wind, cma_pressure AS pressure) AS cma,
       STRUCT(hko_latitude AS latitude, hko_longitude AS longitude, hko_wind AS wind, hko_pressure AS pressure) AS hko,
       STRUCT(newdelhi_latitude AS latitude, newdelhi_longitude AS longitude, newdelhi_wind AS wind, newdelhi_pressure AS pressure) AS newdelhi,
       STRUCT(reunion_latitude AS latitude, reunion_longitude AS longitude, reunion_wind AS wind, reunion_pressure AS pressure) AS reunion,
       STRUCT(bom_latitude AS latitude, bom_longitude AS longitude, bom_wind AS wind, bom_pressure AS pressure) AS bom,
       STRUCT(wellington_latitude AS latitude, wellington_longitude AS longitude, wellington_wind AS wind, wellington_pressure AS pressure) AS wellington,
       STRUCT(nadi_latitude AS latitude, nadi_longitude AS longitude, nadi_wind AS wind, nadi_pressure AS pressure) AS nadi
   ) ORDER BY iso_time ASC ) AS obs
FROM `bigquery-public-data`.noaa_hurricanes.hurricanes
GROUP BY sid, season, number, basin, name

)






-- Queries

-- 1.4s, 41.7 MB
SELECT
  sid, number, basin, name,
  ARRAY_AGG(STRUCT(iso_time, usa_latitude, usa_longitude, usa_wind) ORDER BY usa_wind DESC LIMIT 1)[OFFSET(0)].*
FROM
  `bigquery-public-data`.noaa_hurricanes.hurricanes
WHERE
  season = '2018'
GROUP BY
  sid, number, basin, name
ORDER BY number ASC


-- same as above with nested and with nested_obs
SELECT
  sid, number, basin, name,
  ARRAY_AGG(STRUCT(iso_time, usa.latitude, usa.longitude, usa.wind) ORDER BY usa.wind DESC LIMIT 1)[OFFSET(0)].*
FROM
  ch07.hurricanes_nested
WHERE
  season = '2018'
GROUP BY
  sid, number, basin, name
ORDER BY number ASC




-- 1.0s, 14.7 MB
SELECT
  number, name, basin, (SELECT AS STRUCT iso_time, usa.latitude, usa.longitude, usa.wind FROM UNNEST(obs) ORDER BY usa.wind DESC LIMIT 1).*
FROM ch07.hurricanes_nested_track
WHERE season = '2018'
ORDER BY number ASC


