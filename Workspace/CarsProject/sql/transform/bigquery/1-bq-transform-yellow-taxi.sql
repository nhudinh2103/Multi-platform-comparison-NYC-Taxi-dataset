CREATE OR REPLACE TABLE `amplified-brook-454012-i1`.`nyc_taxi`.`yellow_taxi_trips_transform`
PARTITION BY DATETIME_TRUNC(partition_id, MONTH)
CLUSTER BY trip_year, trip_month, taxi_type, vendor_id
AS
SELECT
  taxi_type,
  CAST(vendor_id AS INT64) AS vendor_id,
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
  CAST(payment_type AS INT64) AS payment_type,
  CAST(trip_year as INT64) as trip_year,
  CAST(trip_month as INT64) as trip_month,
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
  PARSE_DATETIME('%Y%m', FORMAT('%06d', CAST(trip_year as INT64) * 100 + CAST(trip_month as INT64))) as partition_id
FROM (
  SELECT DISTINCT
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
    EXTRACT(YEAR FROM t.pickup_datetime) AS pickup_year,
    EXTRACT(MONTH FROM t.pickup_datetime) AS pickup_month,
    EXTRACT(DAY FROM t.pickup_datetime) AS pickup_day,
    EXTRACT(HOUR FROM t.pickup_datetime) AS pickup_hour,
    EXTRACT(MINUTE FROM t.pickup_datetime) AS pickup_minute,
    EXTRACT(SECOND FROM t.pickup_datetime) AS pickup_second,
    DATE(t.pickup_datetime) AS pickup_date,
    EXTRACT(YEAR FROM t.dropoff_datetime) AS dropoff_year,
    EXTRACT(MONTH FROM t.dropoff_datetime) AS dropoff_month,
    EXTRACT(DAY FROM t.dropoff_datetime) AS dropoff_day,
    EXTRACT(HOUR FROM t.dropoff_datetime) AS dropoff_hour,
    EXTRACT(MINUTE FROM t.dropoff_datetime) AS dropoff_minute,
    EXTRACT(SECOND FROM t.dropoff_datetime) AS dropoff_second,
    DATE(t.dropoff_datetime) AS dropoff_date
  FROM 
    `amplified-brook-454012-i1.nyc_taxi.yellow_taxi_trips_raw` t
    LEFT OUTER JOIN `amplified-brook-454012-i1.nyc_taxi.vendor_lookup` v
      ON (t.vendor_id = v.abbreviation)
    LEFT OUTER JOIN `amplified-brook-454012-i1.nyc_taxi.trip_month_lookup` tm
      ON (t.trip_month = tm.trip_month)
    LEFT OUTER JOIN `amplified-brook-454012-i1.nyc_taxi.payment_type_lookup` pt
      ON (t.payment_type = pt.abbreviation)
    LEFT OUTER JOIN `amplified-brook-454012-i1.nyc_taxi.rate_code_lookup` rc
      ON (t.rate_code_id = rc.rate_code_id)
    LEFT OUTER JOIN `amplified-brook-454012-i1.nyc_taxi.taxi_zone_lookup` tzpu
      ON (t.pickup_location_id = CAST(tzpu.location_id as INT64))
    LEFT OUTER JOIN `amplified-brook-454012-i1.nyc_taxi.taxi_zone_lookup` tzdo
      ON (t.dropoff_location_id = CAST(tzdo.location_id as INT64))
  WHERE
    CAST(t.trip_year AS INT64) < 2015

  UNION ALL

  SELECT DISTINCT
    t.taxi_type,
    CAST(t.vendor_id as INT64),
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
    CAST(t.payment_type as INT64),
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
    EXTRACT(YEAR FROM t.pickup_datetime) AS pickup_year,
    EXTRACT(MONTH FROM t.pickup_datetime) AS pickup_month,
    EXTRACT(DAY FROM t.pickup_datetime) AS pickup_day,
    EXTRACT(HOUR FROM t.pickup_datetime) AS pickup_hour,
    EXTRACT(MINUTE FROM t.pickup_datetime) AS pickup_minute,
    EXTRACT(SECOND FROM t.pickup_datetime) AS pickup_second,
    DATE(t.pickup_datetime) AS pickup_date,
    EXTRACT(YEAR FROM t.dropoff_datetime) AS dropoff_year,
    EXTRACT(MONTH FROM t.dropoff_datetime) AS dropoff_month,
    EXTRACT(DAY FROM t.dropoff_datetime) AS dropoff_day,
    EXTRACT(HOUR FROM t.dropoff_datetime) AS dropoff_hour,
    EXTRACT(MINUTE FROM t.dropoff_datetime) AS dropoff_minute,
    EXTRACT(SECOND FROM t.dropoff_datetime) AS dropoff_second,
    DATE(t.dropoff_datetime) AS dropoff_date
  FROM 
    `amplified-brook-454012-i1.nyc_taxi.yellow_taxi_trips_raw` t
    LEFT OUTER JOIN `amplified-brook-454012-i1.nyc_taxi.vendor_lookup` v
      ON (CAST(t.vendor_id AS INT64) = v.vendor_id)
    LEFT OUTER JOIN `amplified-brook-454012-i1.nyc_taxi.trip_month_lookup` tm
      ON (t.trip_month = tm.trip_month)
    LEFT OUTER JOIN `amplified-brook-454012-i1.nyc_taxi.payment_type_lookup` pt
      ON (CAST(t.payment_type AS INT64) = pt.payment_type)
    LEFT OUTER JOIN `amplified-brook-454012-i1.nyc_taxi.rate_code_lookup` rc
      ON (t.rate_code_id = rc.rate_code_id)
    LEFT OUTER JOIN `amplified-brook-454012-i1.nyc_taxi.taxi_zone_lookup` tzpu
      ON (t.pickup_location_id = CAST(tzpu.location_id as INT64))
    LEFT OUTER JOIN `amplified-brook-454012-i1.nyc_taxi.taxi_zone_lookup` tzdo
      ON (t.dropoff_location_id = CAST(tzdo.location_id as INT64))
  WHERE
    CAST(t.trip_year AS INT64) >= 2015
) curated_data;