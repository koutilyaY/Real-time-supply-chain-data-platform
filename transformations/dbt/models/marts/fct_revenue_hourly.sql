-- Hourly revenue fact, rolled up from 1-minute streaming windows.
select
    region,
    date_trunc('hour', window_start) as revenue_hour,
    sum(order_count)   as orders,
    sum(gross_revenue) as gross_revenue
from {{ ref('stg_orders_revenue') }}
group by 1, 2
