FSCK REPAIR TABLE `synapse_nyc_reference`.`nyctaxi`.green_taxi_trips_raw;

CREATE OR REPLACE TABLE synapse_nyc_reference.nyctaxi.green_taxi_trips_transform
USING DELTA
PARTITIONED BY (trip_year, trip_month)  -- Hive-style partitioning
LOCATION 'abfss://silver@dinhnnpreniumstorage.dfs.core.windows.net/nyctaxi/transform_dw/green-taxi/'
AS
  SELECT /*+ BROADCAST(v), BROADCAST(tm), BROADCAST(pt), BROADCAST(rc), BROADCAST(tzpu), BROADCAST(tzdo) */
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
      t.ehail_fee,
      t.improvement_surcharge,
      t.total_amount,
      t.payment_type,
      t.trip_type,
      t.trip_year,
      t.trip_month,
      v.abbreviation as vendor_abbreviation,
      v.description as vendor_description,
      tt.description as trip_type_description,
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
      year(t.dropoff_datetime) as dropoff_year,
      month(t.dropoff_datetime) as dropoff_month,
      day(t.dropoff_datetime) as dropoff_day,
      hour(t.dropoff_datetime) as dropoff_hour,
      minute(t.dropoff_datetime) as dropoff_minute,
      second(t.dropoff_datetime) as dropoff_second
  FROM 
    synapse_nyc_reference.nyctaxi.green_taxi_trips_raw t
    LEFT OUTER JOIN synapse_nyc_reference.nyctaxi.vendor_lookup v 
      on (t.vendor_id = v.vendor_id)
    LEFT OUTER JOIN synapse_nyc_reference.nyctaxi.trip_type_lookup tt 
      on (t.trip_type = tt.trip_type)
    LEFT OUTER JOIN synapse_nyc_reference.nyctaxi.trip_month_lookup tm 
      on (t.trip_month = tm.trip_month)
    LEFT OUTER JOIN synapse_nyc_reference.nyctaxi.payment_type_lookup pt 
      on (t.payment_type = pt.payment_type)
    LEFT OUTER JOIN synapse_nyc_reference.nyctaxi.rate_code_lookup rc 
      on (t.rate_code_id = rc.rate_code_id)
    LEFT OUTER JOIN synapse_nyc_reference.nyctaxi.taxi_zone_lookup tzpu 
      on (t.pickup_location_id = tzpu.location_id)
    LEFT OUTER JOIN synapse_nyc_reference.nyctaxi.taxi_zone_lookup tzdo 
      on (t.dropoff_location_id = tzdo.location_id);