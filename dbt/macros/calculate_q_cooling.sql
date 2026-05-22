{# Child Macros #}
{% macro h_fg(temp_c) %}
    2501000 - 2361 * {{ temp_c }}
{%- endmacro %}

{% macro p_sat_magnus(temp_c) %}
    610.78 * exp(17.27 * {{ temp_c }} / ({{ temp_c }} + 237.3))
{%- endmacro %}

{% macro w_(rh_pct, p_sat_pa, p_pa) %}
    0.622 * ({{ rh_pct }}/100 * {{ p_sat_pa }}) / ({{ p_pa }} - ({{ rh_pct }}/100 * {{ p_sat_pa }}))
{%- endmacro %}

{% macro _q_total_w(temp_in_c, temp_out_c, w_in, w_out, h_fg, airflow_evap_kg_s, comp_modulation_frac) %}
    {{ comp_modulation_frac }} * {{ airflow_evap_kg_s }} * (1006.0 * ({{ temp_in_c }} - {{ temp_out_c }}) + ({{ h_fg }}) * ({{ w_in }} - {{ w_out }}))
{%- endmacro %}

{# Parent Macro #}
{% macro q_total_w(
    t_evap_inlet_c,
    t_evap_outlet_c,
    rh_evap_inlet_pct,
    rh_evap_outlet_pct,
    pressure_pa,
    airflow_evap_kg_s,
    comp_modulation_frac,
    power_compressor_w,
    q_rated_w
) %}

    {% set h_fg = h_fg(t_evap_outlet_c) %}
    {% set p_sat_in = p_sat_magnus(t_evap_inlet_c) %}
    {% set p_sat_out = p_sat_magnus(t_evap_outlet_c) %}
    {% set w_evap_in = w_(rh_evap_inlet_pct, p_sat_in, pressure_pa) %}
    {% set w_evap_out = w_(rh_evap_outlet_pct, p_sat_out, pressure_pa) %}
    {% set q_total_w =  _q_total_w(t_evap_inlet_c, t_evap_outlet_c, w_evap_in, w_evap_out, h_fg, airflow_evap_kg_s, comp_modulation_frac) %}
    
    least(
        case when {{ q_total_w }} < 0 then 0 else {{ q_total_w }} end,
        {{ q_rated_w }}
    )

{%- endmacro %}
