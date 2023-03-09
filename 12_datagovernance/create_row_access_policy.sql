-- Stored procedure that creates a row access policy in BigQuery based on a region and accounts to authorize
-- The list of accounts must be comma delimited and prefixed with either 'user:', 'group:' or 'serviceAccount:' depending on the type of account. 
-- Be sure to populate the country table before running the stored proc. You can do that as follows:
-- bq query --use_legacy_sql=false < 12_datagovernance/create_populate_country.sql
-- Once you create the stored prcedure, you can call it as follows:
-- CALL `bigquerybook2e.sp.create_row_access_policy`('namer', 'user:anita.example.com,group:fitdw-namer-analysts@example.com');

CREATE OR REPLACE PROCEDURE sp.create_row_access_policy(region STRING, accounts STRING)

BEGIN

DECLARE policy_name STRING;
DECLARE ddl_start, ddl_grant STRING;
DECLARE country_list STRING;
DECLARE country_list_length INT64;

SET policy_name = concat(region, "_policy");
SET ddl_start = concat("create or replace row access policy ", policy_name);
SET ddl_grant = concat(" on bigquerybook2e.fitdw.User grant to ('", accounts, "') filter ");
SET country_list = "";

FOR record IN
  (SELECT country_code FROM fitdw.Country
   WHERE region = region)
DO
  SET country_list = concat(country_list, "'", record.country_code, "', ");
  
END FOR;

SELECT country_list;
SET country_list_length = LENGTH(country_list) - 2;
SET country_list = SUBSTR(country_list, 1, country_list_length);
SELECT country_list;

EXECUTE IMMEDIATE CONCAT(ddl_start, ddl_grant, "using (country_code in (", country_list, "))");

END;





