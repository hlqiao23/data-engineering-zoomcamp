{{ config(materialized="table") }}

with
    tripdata as (select * from {{ ref("stg_fhv_tripdata") }}),

    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')

select

    tripdata.dispatching_base_num,
    tripdata.pickup_datetime,
    tripdata.dropoff_datetime,
    extract(year from tripdata.pickup_datetime) as year,
    extract(month from tripdata.pickup_datetime) as month,
    tripdata.pulocationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    tripdata.dolocationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    tripdata.sr_flag,
    tripdata.affiliated_base_number

from tripdata
inner join dim_zones as pickup_zone on tripdata.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone on tripdata.dolocationid = dropoff_zone.locationid
