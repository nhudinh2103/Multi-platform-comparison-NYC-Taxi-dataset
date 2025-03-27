-- table data is under s3://mydemobucket/delta/my_delta_table directory
create or replace iceberg table `amplified-brook-454012-i1`.`nyc_taxi`.yellow_taxi_trips_raw
  catalog=delta_int
  external_volume = gcs_nyctaxi_silver_delta_vol
  base_location = 'transactions/yellow-taxi';


create or replace iceberg table `amplified-brook-454012-i1`.`nyc_taxi`.green_taxi_trips_raw
  catalog=delta_int
  external_volume = gcs_nyctaxi_silver_delta_vol
  base_location = 'transactions/green-taxi';


-- Create file format for parquet files
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = 'PARQUET';

CREATE STORAGE INTEGRATION gcs_silver_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://nyctaxi-silver');

-- Create stage for external data
-- Note: Replace with your actual storage location
CREATE OR REPLACE STAGE nyctaxi_reference_stage
  URL = 'gcs://nyctaxi-silver/nyctaxi/reference'
  storage_integration = gcs_silver_int
  FILE_FORMAT = parquet_format;

-- 1. Create taxi_zone_lookup table
CREATE OR REPLACE EXTERNAL TABLE taxi_zone_lookup (
  location_id STRING AS (VALUE:location_id::STRING),
  borough STRING AS (VALUE:borough::STRING),
  zone STRING AS (VALUE:zone::STRING),
  service_zone STRING AS (VALUE:service_zone::STRING)
)
WITH LOCATION = @nyctaxi_reference_stage/taxi-zone-lookup/
AUTO_REFRESH = false
FILE_FORMAT = (TYPE = PARQUET);

-- 2. Create trip_month_lookup external table
CREATE OR REPLACE EXTERNAL TABLE trip_month_lookup (
  trip_month STRING AS (VALUE:trip_month::STRING),
  month_name_short STRING AS (VALUE:month_name_short::STRING),
  month_name_full STRING AS (VALUE:month_name_full::STRING)
)
WITH LOCATION = @nyctaxi_reference_stage/trip-month-lookup/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = false;

-- 3. Create rate_code_lookup external table
CREATE OR REPLACE EXTERNAL TABLE rate_code_lookup (
  rate_code_id INT AS (VALUE:rate_code_id::INT),
  description STRING AS (VALUE:description::STRING)
)
WITH LOCATION = @nyctaxi_reference_stage/rate-code-lookup/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = false;

-- 4. Create payment_type_lookup external table
CREATE OR REPLACE EXTERNAL TABLE payment_type_lookup (
  payment_type INT AS (VALUE:payment_type::INT),
  abbreviation STRING AS (VALUE:abbreviation::STRING),
  description STRING AS (VALUE:description::STRING)
)
WITH LOCATION = @nyctaxi_reference_stage/payment-type-lookup/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = false;

-- 5. Create trip_type_lookup external table
CREATE OR REPLACE EXTERNAL TABLE trip_type_lookup (
  trip_type INT AS (VALUE:trip_type::INT),
  description STRING AS (VALUE:description::STRING)
)
WITH LOCATION = @nyctaxi_reference_stage/trip-type-lookup/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = false;

-- 6. Create vendor_lookup external table
CREATE OR REPLACE EXTERNAL TABLE vendor_lookup (
  vendor_id INT AS (VALUE:vendor_id::INT),
  abbreviation STRING AS (VALUE:abbreviation::STRING),
  description STRING AS (VALUE:description::STRING)
)
WITH LOCATION = @nyctaxi_reference_stage/vendor-lookup/
FILE_FORMAT = (TYPE = PARQUET)
AUTO_REFRESH = false;