SET use_cached_result = false;
-- Q1 - Original query from workshop
select distinct t.taxi_type,
      t.vendor_id as vendor_id,
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
      v.abbreviation as vendor_abbreviation,
      v.description as vendor_description,
      tm.month_name_short,
      tm.month_name_full,
      pt.description as payment_type_description,
      rc.description as rate_code_description,
      tzpu.borough as pickup_borough,
      tzpu.zone as pickup_zone,
      tzpu.service_zone as pickup_service_zone,
      tzdo.borough as dropoff_borough,
      tzdo.zone as dropoff_zone,
      tzdo.service_zone as dropoff_service_zone,
      year(t.pickup_datetime) as pickup_year,
      month(t.pickup_datetime) as pickup_month,
      day(t.pickup_datetime) as pickup_day,
      hour(t.pickup_datetime) as pickup_hour,
      minute(t.pickup_datetime) as pickup_minute,
      second(t.pickup_datetime) as pickup_second,
      date(t.pickup_datetime) as pickup_date,
      year(t.dropoff_datetime) as dropoff_year,
      month(t.dropoff_datetime) as dropoff_month,
      day(t.dropoff_datetime) as dropoff_day,
      hour(t.dropoff_datetime) as dropoff_hour,
      minute(t.dropoff_datetime) as dropoff_minute,
      second(t.dropoff_datetime) as dropoff_second,
      date(t.dropoff_datetime) as dropoff_date
  from 
    synapse_nyc_reference.nyctaxi.yellow_taxi_trips_raw t
    left outer join synapse_nyc_reference.nyctaxi.vendor_lookup v 
      on (case when t.trip_year < "2015" then t.vendor_id = v.abbreviation else t.vendor_id = v.vendor_id end)
    left outer join synapse_nyc_reference.nyctaxi.trip_month_lookup tm 
      on (t.trip_month = tm.trip_month)
    left outer join synapse_nyc_reference.nyctaxi.payment_type_lookup pt 
      on (case when t.trip_year < "2015" then t.payment_type = pt.abbreviation else t.payment_type = pt.payment_type end)
    left outer join synapse_nyc_reference.nyctaxi.rate_code_lookup rc 
      on (t.rate_code_id = rc.rate_code_id)
    left outer join synapse_nyc_reference.nyctaxi.taxi_zone_lookup tzpu 
      on (t.pickup_location_id = tzpu.location_id)
    left outer join synapse_nyc_reference.nyctaxi.taxi_zone_lookup tzdo 
      on (t.dropoff_location_id = tzdo.location_id);

SET use_cached_result = false;
-- Q2 - Original query with broadcast hint
select /*+ BROADCAST(v), BROADCAST(tm), BROADCAST(pt), BROADCAST(rc), BROADCAST(tzpu), BROADCAST(tzdo) */ distinct t.taxi_type,
      t.vendor_id as vendor_id,
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
      v.abbreviation as vendor_abbreviation,
      v.description as vendor_description,
      tm.month_name_short,
      tm.month_name_full,
      pt.description as payment_type_description,
      rc.description as rate_code_description,
      tzpu.borough as pickup_borough,
      tzpu.zone as pickup_zone,
      tzpu.service_zone as pickup_service_zone,
      tzdo.borough as dropoff_borough,
      tzdo.zone as dropoff_zone,
      tzdo.service_zone as dropoff_service_zone,
      year(t.pickup_datetime) as pickup_year,
      month(t.pickup_datetime) as pickup_month,
      day(t.pickup_datetime) as pickup_day,
      hour(t.pickup_datetime) as pickup_hour,
      minute(t.pickup_datetime) as pickup_minute,
      second(t.pickup_datetime) as pickup_second,
      date(t.pickup_datetime) as pickup_date,
      year(t.dropoff_datetime) as dropoff_year,
      month(t.dropoff_datetime) as dropoff_month,
      day(t.dropoff_datetime) as dropoff_day,
      hour(t.dropoff_datetime) as dropoff_hour,
      minute(t.dropoff_datetime) as dropoff_minute,
      second(t.dropoff_datetime) as dropoff_second,
      date(t.dropoff_datetime) as dropoff_date
  from 
    synapse_nyc_reference.nyctaxi.yellow_taxi_trips_raw t
    left outer join synapse_nyc_reference.nyctaxi.vendor_lookup v 
      on (case when t.trip_year < "2015" then t.vendor_id = v.abbreviation else t.vendor_id = v.vendor_id end)
    left outer join synapse_nyc_reference.nyctaxi.trip_month_lookup tm 
      on (t.trip_month = tm.trip_month)
    left outer join synapse_nyc_reference.nyctaxi.payment_type_lookup pt 
      on (case when t.trip_year < "2015" then t.payment_type = pt.abbreviation else t.payment_type = pt.payment_type end)
    left outer join synapse_nyc_reference.nyctaxi.rate_code_lookup rc 
      on (t.rate_code_id = rc.rate_code_id)
    left outer join synapse_nyc_reference.nyctaxi.taxi_zone_lookup tzpu 
      on (t.pickup_location_id = tzpu.location_id)
    left outer join synapse_nyc_reference.nyctaxi.taxi_zone_lookup tzdo 
      on (t.dropoff_location_id = tzdo.location_id);

SET use_cached_result = false;
-- Q3 - Union all query
SELECT DISTINCT
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

SELECT DISTINCT
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
  t.trip_year >= 2015;


-- Q4 - Union all query with broadcast join hint
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
  t.trip_year >= 2015;