{% macro parameter_abuse_flag(reading, low_threshold, high_threshold) %}
    case
        when {{ reading }} is null then null
        when {{ reading }} <= {{ low_threshold }} then 'low'
        when {{ reading }} >= {{ high_threshold }} then 'high'
        else 'optimal'
    end
{%- endmacro %}

{% macro has_damage_flag(minutes_damage, minutes_damage_threshold) %}
    case
        when {{ minutes_damage }} >= {{ minutes_damage_threshold }} then true else false
    end
{%- endmacro %}

{# state duration macros #}

{% macro consecutive_timestamp_diff(timestamp_col, granularity, unique_key) %}
    timestamp_diff(
        {{ timestamp_col }},
        lag({{ timestamp_col }}) over (
            partition by {{ unique_key }}
            order by {{ timestamp_col }}
        ),
        {{ granularity }}
    )
{%- endmacro %}

{% macro route_valid_duration(status_col, target, timestamp_col, max_gap, granularity, unique_key) %}
    case
        when lag({{ status_col }}) over (
            partition by {{ unique_key }}
            order by {{ timestamp_col }}
        ) = {{ target }}
         and {{ consecutive_timestamp_diff(timestamp_col, granularity, unique_key) }} <= {{ max_gap }}
            then {{ consecutive_timestamp_diff(timestamp_col, granularity, unique_key) }}
        else 0
    end 
{%- endmacro %}

{% macro route_downtime_duration(status_col, timestamp_col, max_gap, granularity, unique_key) %}
    case
        when lag({{ status_col }}) over (
            partition by {{ unique_key }}
            order by {{ timestamp_col }}
        ) is not null
         and {{ consecutive_timestamp_diff(timestamp_col, granularity, unique_key) }} > {{ max_gap }}
            then {{ consecutive_timestamp_diff(timestamp_col, granularity, unique_key) }}
        else 0
    end
{%- endmacro %}

