-- Hourly IoT metric rollup per warehouse + metric.
select
    warehouse_id,
    metric,
    date_trunc('hour', window_start) as reading_hour,
    sum(reading_count)               as readings,
    avg(avg_value)                   as avg_value,
    min(min_value)                   as min_value,
    max(max_value)                   as max_value
from {{ ref('stg_iot_metrics') }}
group by 1, 2, 3
