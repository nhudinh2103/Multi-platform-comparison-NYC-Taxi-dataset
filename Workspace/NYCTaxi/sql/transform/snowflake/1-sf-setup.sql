/*--
In this Worksheet we will walk through templated SQL for the end to end process required
to load data from Google Cloud into a table.

    Helpful Snowflake Documentation:
        Bulk Loading from Google Cloud Storage - https://docs.snowflake.com/en/user-guide/data-load-gcs
--*/


-------------------------------------------------------------------------------------------
    -- Step 1: To start, let's set the Role and Warehouse context
        -- USE ROLE: https://docs.snowflake.com/en/sql-reference/sql/use-role
        -- USE WAREHOUSE: https://docs.snowflake.com/en/sql-reference/sql/use-warehouse
-------------------------------------------------------------------------------------------

--> To run a single query, place your cursor in the query editor and select the Run button (⌘-Return).
--> To run the entire worksheet, select 'Run All' from the dropdown next to the Run button (⌘-Shift-Return).

---> set Role Context
USE ROLE accountadmin;

---> set Warehouse Context
USE WAREHOUSE compute_wh;


-------------------------------------------------------------------------------------------
    -- Step 2: Create Database
        -- CREATE DATABASE: https://docs.snowflake.com/en/sql-reference/sql/create-database
-------------------------------------------------------------------------------------------

---> create the Database
CREATE DATABASE IF NOT EXISTS `amplified-brook-454012-i1`;

-------------------------------------------------------------------------------------------
    -- Step 3: Create Schema
        -- CREATE SCHEMA: https://docs.snowflake.com/en/sql-reference/sql/create-schema
-------------------------------------------------------------------------------------------

---> create the Schema
CREATE SCHEMA IF NOT EXISTS `amplified-brook-454012-i1`.`nyc_taxi`;

DROP EXTERNAL VOLUME gcs_nyctaxi_silver_external;

CREATE EXTERNAL VOLUME gcs_nyctaxi_silver_delta_vol
  STORAGE_LOCATIONS =
    (
      (
        NAME = 'gcs-nyctaxi-silver-delta'
        STORAGE_PROVIDER = 'GCS'
        STORAGE_BASE_URL = 'gcs://nyctaxi-silver/nyctaxi'
      )
    );

desc external volume gcs_nyctaxi_silver_delta_vol;

{"NAME":"gcs-nyctaxi-silver-delta","STORAGE_PROVIDER":"GCS",
"STORAGE_BASE_URL":"gcs://nyctaxi-silver/nyctaxi",
"STORAGE_ALLOWED_LOCATIONS":["gcs://nyctaxi-silver/nyctaxi*"],
"STORAGE_GCP_SERVICE_ACCOUNT":"<created_service_account>",
"ENCRYPTION_TYPE":"NONE","ENCRYPTION_KMS_KEY_ID":""}

-- assign created_service_account with correspondence role based on this link
-- https://docs.snowflake.com/en/user-guide/data-load-gcs-config

-- Check snowflake can access to gcs
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('gcs_nyctaxi_silver_delta_vol');

CREATE OR REPLACE CATALOG INTEGRATION delta_int
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = DELTA
  ENABLED=true;

/* Grant usage to the role where Data engg can create table */
GRANT USAGE ON external volume gcs_nyctaxi_silver_delta_vol to role sysadmin;
GRANT USAGE ON  INTEGRATION  delta_int to role sysadmin;
GRANT EXECUTE MANAGED TASK on account to role sysadmin;

use role accountadmin;
use warehouse compute_wh;