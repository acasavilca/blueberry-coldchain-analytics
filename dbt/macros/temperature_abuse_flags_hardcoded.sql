{% macro temp_abuse_flag_hardcode(temp_c, fruit_type) %}
    case
        when {{ fruit_type }} = 'blueberry' then
            case
                when {{ temp_c }} >= 2 then 'potential_heat_damage'
                when {{ temp_c }} <= -0.5 then 'potential_freeze_damage'
                else 'safe_temperature'
            end
            
        when {{ fruit_type }} = 'avocado' then
            case
                when {{ temp_c }} >= 7 then 'potential_heat_damage'
                when {{ temp_c }} <= 3 then 'potential_freeze_damage'
                else 'safe_temperature'
            end
        else 'unknown_fruit_rules'
    end
{%- endmacro %}

{% macro has_heat_damage_flag_hardcode(minutes_heat_damage, fruit_type) %}
    case
        when {{ fruit_type }} = 'blueberry' then
            case
                when {{ minutes_heat_damage }} >= 120 then true else false
            end
        when {{ fruit_type }} = 'avocado' then
            case
                when {{ minutes_heat_damage }} >= 120 then true else false
            end
    end
{%- endmacro %}

{% macro has_chill_damage_flag_hardcode(minutes_chill_damage, fruit_type) %}
    case
        when {{ fruit_type }} = 'blueberry' then
            case
                when {{ minutes_chill_damage }} >= 10 then true else false
            end
        when {{ fruit_type }} = 'avocado' then
            case
                when {{ minutes_chill_damage }} >= 24*60 then true else false
            end
    end
{%- endmacro %}