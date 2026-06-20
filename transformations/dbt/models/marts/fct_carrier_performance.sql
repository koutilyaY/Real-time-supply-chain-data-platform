-- Carrier on-time performance derived from 5-minute delay windows.
select
    carrier,
    sum(total_shipments)                                          as total_shipments,
    sum(delayed_shipments)                                        as delayed_shipments,
    cast(sum(delayed_shipments) as double) / nullif(sum(total_shipments), 0) as delay_rate,
    1 - cast(sum(delayed_shipments) as double) / nullif(sum(total_shipments), 0) as on_time_rate
from {{ ref('stg_shipment_delays') }}
group by 1
