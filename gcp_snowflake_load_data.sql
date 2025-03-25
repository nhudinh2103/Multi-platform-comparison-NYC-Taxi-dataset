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
CREATE [ OR REPLACE ] DATABASE [ IF NOT EXISTS ] <database_name>
    [ COMMENT = '<string_literal>' ];


-------------------------------------------------------------------------------------------
    -- Step 3: Create Schema
        -- CREATE SCHEMA: https://docs.snowflake.com/en/sql-reference/sql/create-schema
-------------------------------------------------------------------------------------------

---> create the Schema
CREATE [ OR REPLACE ] SCHEMA [ IF NOT EXISTS ] <database_name>.<schema_name>
  [ COMMENT = '<string_literal>' ];


-------------------------------------------------------------------------------------------
    -- Step 4: Create Table
        -- CREATE TABLE: https://docs.snowflake.com/en/sql-reference/sql/create-table
-------------------------------------------------------------------------------------------

---> create the Table
CREATE [ OR REPLACE ] TABLE [ IF NOT EXISTS ] <database_name>.<schema_name>.<table_name>
    (
    <col1_name> <COL1_TYPE>
    ,<col2_name> <COL2_TYPE>
    --> supported types: https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
    )
    [COMMENT = '<string_literal>'];

---> query the empty Table
SELECT * FROM <database_name>.<schema_name>.<table_name>;


-------------------------------------------------------------------------------------------
    -- Step 5: Create Storage Integrations
        -- CREATE STORAGE INTEGRATION: https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration
-------------------------------------------------------------------------------------------

    /*--
      A Storage Integration is a Snowflake object that stores a generated identity and access management
      (IAM) entity for your external cloud storage, along with an optional set of allowed or blocked storage locations
      (Amazon S3, Google Cloud Storage, or Microsoft Azure).
    --*/

---> Create the Google Cloud Storage Integration
    -- Configuring an Integration for Google Cloud Storage: https://docs.snowflake.com/en/user-guide/data-load-gcs-config
    
CREATE [ OR REPLACE ] STORAGE INTEGRATION [ IF NOT EXISTS ] <gcs_integration_name>
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = { TRUE | FALSE }
  STORAGE_ALLOWED_LOCATIONS = ('gcs://<bucket>/<path>/' [ , 'gcs://<bucket>/<path>/' ... ] )
  [ STORAGE_BLOCKED_LOCATIONS = ('gcs://<bucket>/<path>/' [ , 'gcs://<bucket>/<path>/' ... ] ) ]
  [ COMMENT = '<string_literal>' ];

    /*--
      Execute the command below to retrive the ID for the Cloud Storage Service Account that was created automatically for your Snowflake account.
      You'll use these values to configure permissions for Snowflake in your GCP Management Console:
          https://docs.snowflake.com/en/user-guide/data-load-gcs-config#step-2-retrieve-the-cloud-storage-service-account-for-your-snowflake-account
    --*/

---> Describe our Integration
    -- DESCRIBE INTEGRATIONS: https://docs.snowflake.com/en/sql-reference/sql/desc-integration
DESCRIBE INTEGRATION <gcs_integration_name>;


---> View our Storage Integrations
    -- SHOW INTEGRATIONS: https://docs.snowflake.com/en/sql-reference/sql/show-integrations

SHOW STORAGE INTEGRATIONS;


-------------------------------------------------------------------------------------------
    -- Step 6: Create Stage Objects
-------------------------------------------------------------------------------------------

    /*--
      A stage specifies where data files are stored (i.e. "staged") so that the data in the files
      can be loaded into a table.
    --*/

---> Create the Google Cloud Storage Stage
    -- Create a Google Cloud Stage: https://docs.snowflake.com/en/user-guide/data-load-gcs-config#create-an-external-stage-using-sql

CREATE [ OR REPLACE ] STAGE [ IF NOT EXISTS ] <gcp_stage_name>
URL = { 'gcs://<bucket>[/<path>/]' | 'gcs://<bucket>[/<path>/]' }
STORAGE_INTEGRATION = <gcp_integration_name> -- created in previous step
[ FILE_FORMAT = ( { FORMAT_NAME = '<file_format_name>' | TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ]
[ COMMENT = '<string_literal>' ];


---> View our Stages
    -- SHOW STAGES: https://docs.snowflake.com/en/sql-reference/sql/show-stages

SHOW STAGES;


-------------------------------------------------------------------------------------------
    -- Step 7: Load Data from Stages
-------------------------------------------------------------------------------------------

---> Load data from the Google Cloud Stage into the Table
    -- Copying Data from a Google Cloud Storage Stage: https://docs.snowflake.com/en/user-guide/data-load-gcs-copy
    -- COPY INTO <table>: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table

COPY INTO <database_name>.<schema_name>.<table_name>
  FROM @<gcp_stage_name>
    [ FILES = ( '<file_name>' [ , '<file_name>' ] [ , ... ] ) ]
    [ PATTERN = '<regex_pattern>' ]
    [ FILE_FORMAT = ( { FORMAT_NAME = '[<namespace>.]<file_format_name>' |
                        TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ] } ) ];


-------------------------------------------------------------------------------------------
    -- Step 8: Start querying your Data!
-------------------------------------------------------------------------------------------

---> Great job! You just successfully loaded data from Google Cloud Storage into a Snowflake table
---> through an external stage. You can now start querying or analyzing the data.

SELECT * FROM <database_name>.<schema_name>.<table_name>;
