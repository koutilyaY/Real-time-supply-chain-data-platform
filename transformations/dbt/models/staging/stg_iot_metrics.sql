-- Dedup to one row per (warehouse_id, metric, window_start); keep most complete.
with src as (
    select
        warehouse_id,
        metric,
        window_start,
        window_end,
        reading_count,
        avg_value,
        min_value,
        max_value,
        row_number() over (
            partition by warehouse_id, metric, window_start
            order by reading_count desc
        ) as _rn
    from {{ source('silver', 'iot_metric_1m') }}
)
select warehouse_id, metric, window_start, window_end,
       reading_count, avg_value, min_value, max_value
from src
where _rn = 1
