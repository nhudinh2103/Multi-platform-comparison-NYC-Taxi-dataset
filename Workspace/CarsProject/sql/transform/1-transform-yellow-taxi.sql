FSCK REPAIR TABLE nyctaxi.yellow_taxi_trips_raw;

CREATE OR REPLACE TABLE `synapse_nyc_reference`.`nyctaxi`.yellow_taxi_trips_transform
USING DELTA
PARTITIONED BY (trip_year, trip_month)  -- Hive-style partitioning
LOCATION 'abfss://curated@dinhnnpreniumstorage.dfs.core.windows.net/nyctaxi/transform_dw/yellow-taxi/'
AS
SELECT
  taxi_type,
  CAST(vendor_id AS INT) AS vendor_id,
  pickup_datetime,
  dropoff_datetime,
  store_and_fwd_flag,
  rate_code_id,
  pickup_location_id,
  dropoff_location_id,
  pickup_longitude,
  pickup_latitude,
  dropoff_longitude,
  dropoff_latitude,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  CAST(payment_type AS INT) AS payment_type,
  trip_year,
  trip_month,
  vendor_abbreviation,
  vendor_description,
  month_name_short,
  month_name_full,
  payment_type_description,
  rate_code_description,
  pickup_borough,
  pickup_zone,
  pickup_service_zone,
  dropoff_borough,
  dropoff_zone,
  dropoff_service_zone,
  pickup_year,
  pickup_month,
  pickup_day,
  pickup_hour,
  pickup_minute,
  pickup_second,
  pickup_date,
  dropoff_year,
  dropoff_month,
  dropoff_day,
  dropoff_hour,
  dropoff_minute,
  dropoff_second,
  dropoff_date,
  partition_id
FROM (
  SELECT /*+ BROADCAST(v), BROADCAST(tm), BROADCAST(pt), BROADCAST(rc), BROADCAST(tzpu), BROADCAST(tzdo) */ DISTINCT
    t.taxi_type,
    v.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.store_and_fwd_flag,
    t.rate_code_id,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.pickup_longitude,
    t.pickup_latitude,
    t.dropoff_longitude,
    t.dropoff_latitude,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    pt.payment_type,
    t.trip_year,
    t.trip_month,
    v.abbreviation AS vendor_abbreviation,
    v.description AS vendor_description,
    tm.month_name_short,
    tm.month_name_full,
    pt.description AS payment_type_description,
    rc.description AS rate_code_description,
    tzpu.borough AS pickup_borough,
    tzpu.zone AS pickup_zone,
    tzpu.service_zone AS pickup_service_zone,
    tzdo.borough AS dropoff_borough,
    tzdo.zone AS dropoff_zone,
    tzdo.service_zone AS dropoff_service_zone,
    year(t.pickup_datetime) AS pickup_year,
    month(t.pickup_datetime) AS pickup_month,
    day(t.pickup_datetime) AS pickup_day,
    hour(t.pickup_datetime) AS pickup_hour,
    minute(t.pickup_datetime) AS pickup_minute,
    second(t.pickup_datetime) AS pickup_second,
    date(t.pickup_datetime) AS pickup_date,
    year(t.dropoff_datetime) AS dropoff_year,
    month(t.dropoff_datetime) AS dropoff_month,
    day(t.dropoff_datetime) AS dropoff_day,
    hour(t.dropoff_datetime) AS dropoff_hour,
    minute(t.dropoff_datetime) AS dropoff_minute,
    second(t.dropoff_datetime) AS dropoff_second,
    date(t.dropoff_datetime) AS dropoff_date,
    unix_millis(t.pickup_datetime) as partition_id
  FROM 
    `synapse_nyc_reference`.`nyctaxi`.yellow_taxi_trips_raw t
    LEFT OUTER JOIN `synapse_nyc_reference`.`nyctaxi`.vendor_lookup v
      ON (t.vendor_id = v.abbreviation)
    LEFT OUTER JOIN `synapse_nyc_reference`.`nyctaxi`.trip_month_lookup tm
      ON (t.trip_month = tm.trip_month)
    LEFT OUTER JOIN `synapse_nyc_reference`.`nyctaxi`.payment_type_lookup pt
      ON (t.payment_type = pt.abbreviation)
    LEFT OUTER JOIN `synapse_nyc_reference`.`nyctaxi`.rate_code_lookup rc
      ON (t.rate_code_id = rc.rate_code_id)
    LEFT OUTER JOIN `synapse_nyc_reference`.`nyctaxi`.taxi_zone_lookup tzpu
      ON (t.pickup_location_id = tzpu.location_id)
    LEFT OUTER JOIN `synapse_nyc_reference`.`nyctaxi`.taxi_zone_lookup tzdo
      ON (t.dropoff_location_id = tzdo.location_id)
  WHERE
    t.trip_year < 2015

  UNION ALL

  SELECT /*+ BROADCAST(v), BROADCAST(tm), BROADCAST(pt), BROADCAST(rc), BROADCAST(tzpu), BROADCAST(tzdo) */ DISTINCT
    t.taxi_type,
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.store_and_fwd_flag,
    t.rate_code_id,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.pickup_longitude,
    t.pickup_latitude,
    t.dropoff_longitude,
    t.dropoff_latitude,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.payment_type,
    t.trip_year,
    t.trip_month,
    v.abbreviation AS vendor_abbreviation,
    v.description AS vendor_description,
    tm.month_name_short,
    tm.month_name_full,
    pt.description AS payment_type_description,
    rc.description AS rate_code_description,
    tzpu.borough AS pickup_borough,
    tzpu.zone AS pickup_zone,
    tzpu.service_zone AS pickup_service_zone,
    tzdo.borough AS dropoff_borough,
    tzdo.zone AS dropoff_zone,
    tzdo.service_zone AS dropoff_service_zone,
    year(t.pickup_datetime) AS pickup_year,
    month(t.pickup_datetime) AS pickup_month,
    day(t.pickup_datetime) AS pickup_day,
    hour(t.pickup_datetime) AS pickup_hour,
    minute(t.pickup_datetime) AS pickup_minute,
    second(t.pickup_datetime) AS pickup_second,
    date(t.pickup_datetime) AS pickup_date,
    year(t.dropoff_datetime) AS dropoff_year,
    month(t.dropoff_datetime) AS dropoff_month,
    day(t.dropoff_datetime) AS dropoff_day,
    hour(t.dropoff_datetime) AS dropoff_hour,
    minute(t.dropoff_datetime) AS dropoff_minute,
    second(t.dropoff_datetime) AS dropoff_second,
    date(t.dropoff_datetime) AS dropoff_date,
    unix_millis(t.pickup_datetime) as partition_id
  FROM 
    `synapse_nyc_reference`.`nyctaxi`.yellow_taxi_trips_raw t
    LEFT OUTER JOIN `synapse_nyc_reference`.`nyctaxi`.vendor_lookup v
      ON (t.vendor_id = v.vendor_id)
    LEFT OUTER JOIN `synapse_nyc_reference`.`nyctaxi`.trip_month_lookup tm
      ON (t.trip_month = tm.trip_month)
    LEFT OUTER JOIN `synapse_nyc_reference`.`nyctaxi`.payment_type_lookup pt
      ON (t.payment_type = pt.payment_type)
    LEFT OUTER JOIN `synapse_nyc_reference`.`nyctaxi`.rate_code_lookup rc
      ON (t.rate_code_id = rc.rate_code_id)
    LEFT OUTER JOIN `synapse_nyc_reference`.`nyctaxi`.taxi_zone_lookup tzpu
      ON (t.pickup_location_id = tzpu.location_id)
    LEFT OUTER JOIN `synapse_nyc_reference`.`nyctaxi`.taxi_zone_lookup tzdo
      ON (t.dropoff_location_id = tzdo.location_id)
  WHERE
    t.trip_year >= 2015
) curated_data