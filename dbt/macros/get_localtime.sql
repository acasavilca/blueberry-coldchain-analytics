{% macro get_localtime(timestamp_columns, timezone_id) %}
    {% for timestamp_col in timestamp_columns %}
        datetime({{ timestamp_col }}, {{ timezone_id }}) as {{ timestamp_col.split('.')[-1] }}_localtime,
    {% endfor %}
{%- endmacro %}