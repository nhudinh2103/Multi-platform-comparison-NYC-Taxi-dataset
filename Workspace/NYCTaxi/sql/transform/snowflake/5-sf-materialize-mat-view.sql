CREATE OR REPLACE TABLE taxi_trips_mat_view
CLUSTER BY (trip_year, trip_month, taxi_type, vendor_id)
AS
SELECT 
    *,
    DATE_TRUNC('MONTH', TO_TIMESTAMP(CONCAT(LPAD(CAST(trip_year AS VARCHAR), 4, '0'), 
                                           LPAD(CAST(trip_month AS VARCHAR), 2, '0'), 
                                           '01'), 'YYYYMMDD')) as partition_id 
FROM (
    SELECT DISTINCT  
        taxi_type,
        vendor_id,
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
        0.0 AS ehail_fee, -- Added inline
        improvement_surcharge,
        total_amount,
        payment_type,
        0 AS trip_type, -- Added inline
        vendor_abbreviation,
        vendor_description,
        '' AS trip_type_description, -- Added inline
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
        dropoff_year,
        dropoff_month,
        dropoff_day,
        dropoff_hour,
        dropoff_minute,
        dropoff_second,
        trip_year,
        trip_month
    FROM yellow_taxi_trips_transform
    
    UNION ALL
    
    SELECT DISTINCT 
        taxi_type,
        vendor_id,
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
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        trip_type,
        vendor_abbreviation,
        vendor_description,
        trip_type_description,
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
        dropoff_year,
        dropoff_month,
        dropoff_day,
        dropoff_hour,
        dropoff_minute,
        dropoff_second,
        trip_year,
        trip_month
    FROM green_taxi_trips_transform
) tmp;