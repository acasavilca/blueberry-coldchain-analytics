{% macro temp_abuse_flag(temp_c, heat_damage_temp_c, freeze_damage_temp_c) %}
    case
        when {{ temp_c }} >= {{ heat_damage_temp_c }} then 'potential_heat_damage'
        when {{ temp_c }} <= {{ freeze_damage_temp_c }} then 'potential_freeze_damage'
        else 'safe_temperature'
    end
{%- endmacro %}

{% macro has_temp_damage_flag(minutes_damage, minutes_damage_threshold) %}
    case
        when {{ minutes_damage }} >= {{ minutes_damage_threshold }} then true else false
    end
{%- endmacro %}
