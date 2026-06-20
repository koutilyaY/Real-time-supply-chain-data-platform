-- Dedup the streaming window output to exactly one row per (region, window_start).
-- Flink delivers at-least-once, so a job restart/replay can re-emit the same
-- window; we keep the most-complete emission (highest order_count) so Gold
-- aggregates never double-count.
with src as (
    select
        region,
        window_start,
        window_end,
        order_count,
        gross_revenue,
        row_number() over (
            partition by region, window_start
            order by order_count desc, gross_revenue desc
        ) as _rn
    from {{ source('silver', 'orders_revenue_1m') }}
)
select region, window_start, window_end, order_count, gross_revenue
from src
where _rn = 1
