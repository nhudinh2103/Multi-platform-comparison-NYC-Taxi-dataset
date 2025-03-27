CREATE OR REPLACE TABLE green_taxi_trips_transform
CLUSTER BY (trip_year, trip_month, taxi_type, vendor_id)
AS
  SELECT
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
      CAST(t.trip_year AS INT) as trip_year,
      CAST(t.trip_month AS INT) as trip_month,
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
      EXTRACT(YEAR FROM t.pickup_datetime) as pickup_year,
      EXTRACT(MONTH FROM t.pickup_datetime) as pickup_month,
      EXTRACT(DAY FROM t.pickup_datetime) as pickup_day,
      EXTRACT(HOUR FROM t.pickup_datetime) as pickup_hour,
      EXTRACT(MINUTE FROM t.pickup_datetime) as pickup_minute,
      EXTRACT(SECOND FROM t.pickup_datetime) as pickup_second,
      EXTRACT(YEAR FROM t.dropoff_datetime) as dropoff_year,
      EXTRACT(MONTH FROM t.dropoff_datetime) as dropoff_month,
      EXTRACT(DAY FROM t.dropoff_datetime) as dropoff_day,
      EXTRACT(HOUR FROM t.dropoff_datetime) as dropoff_hour,
      EXTRACT(MINUTE FROM t.dropoff_datetime) as dropoff_minute,
      EXTRACT(SECOND FROM t.dropoff_datetime) as dropoff_second
  FROM 
    green_taxi_trips_raw t
    LEFT OUTER JOIN vendor_lookup v
      ON (CAST(t.vendor_id AS INT) = v.vendor_id)
    LEFT OUTER JOIN trip_type_lookup tt 
      ON (t.trip_type = tt.trip_type)
    LEFT OUTER JOIN trip_month_lookup tm
      ON (t.trip_month = tm.trip_month)
    LEFT OUTER JOIN payment_type_lookup pt
      ON (CAST(t.payment_type AS INT) = pt.payment_type)
    LEFT OUTER JOIN rate_code_lookup rc
      ON (t.rate_code_id = rc.rate_code_id)
    LEFT OUTER JOIN taxi_zone_lookup tzpu
      ON (t.pickup_location_id = CAST(tzpu.location_id AS INT))
    LEFT OUTER JOIN taxi_zone_lookup tzdo
      ON (t.dropoff_location_id = CAST(tzdo.location_id AS INT));