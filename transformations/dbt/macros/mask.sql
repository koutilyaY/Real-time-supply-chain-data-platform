{# Column masking helpers for governance (Trino SQL).
   Usage in a model:  {{ mask_hash('customer_id') }} as customer_id
   The salt should come from an env var in real deployments. #}

{% macro mask_hash(column, salt=none) -%}
    {%- set s = salt if salt is not none else var('mask_salt', 'dev-salt') -%}
    lower(to_hex(sha256(to_utf8(concat(cast({{ column }} as varchar), '{{ s }}')))))
{%- endmacro %}

{% macro mask_redact(column) -%}
    cast('***' as varchar)
{%- endmacro %}
