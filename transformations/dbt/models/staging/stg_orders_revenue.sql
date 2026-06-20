select
    region,
    window_start,
    window_end,
    order_count,
    gross_revenue
from {{ source('silver', 'orders_revenue_1m') }}
