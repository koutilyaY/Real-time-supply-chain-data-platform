select
    warehouse_id,
    metric,
    window_start,
    window_end,
    reading_count,
    avg_value,
    min_value,
    max_value
from {{ source('silver', 'iot_metric_1m') }}
