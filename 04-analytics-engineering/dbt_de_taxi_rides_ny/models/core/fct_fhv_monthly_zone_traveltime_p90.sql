{{ config(materialized="table") }}

with
    trip_duration_calculated as (
        select
            *,
            timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration
        from {{ ref("dim_fhv_trips") }}
    )

select

    *,
    percentile_cont(trip_duration, 0.90) over (
        partition by year, month, pulocationid, dolocationid
    ) as trip_duration_p90

from trip_duration_calculated
