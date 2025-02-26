{{ config(materialized="table") }}

with
    trips_data as (
        select *, format_date('%Y-%Q', date(pickup_datetime)) as year_quarter
        from {{ ref("fact_trips") }}
    ),
    quarterly_revenue as (
        select
            -- Revenue grouping 
            service_type,
            year_quarter,
            sum(total_amount) as revenue_quarterly_total_amount
        from trips_data
        group by 1, 2
    )
select
    service_type,
    year_quarter,
    revenue_quarterly_total_amount,
    lag(revenue_quarterly_total_amount) over (
        partition by service_type order by year_quarter
    ) as previous_revenue_quarterly_total_amount,
    (
        revenue_quarterly_total_amount - lag(revenue_quarterly_total_amount) over (partition by service_type order by year_quarter)
    ) / nullif(
        lag(revenue_quarterly_total_amount) over (partition by service_type order by year_quarter), 0
    ) as qoq_growth
from quarterly_revenue
order by 1, 2
