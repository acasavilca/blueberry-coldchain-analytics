{% macro calculate_cop(
    t_evap_inlet_c,
    t_evap_outlet_c,
    rh_evap_inlet_pct,
    rh_evap_outlet_pct,
    pressure_pa,
    airflow_evap_kg_s,
    power_compressor_w
) %}

    {% set h_fg = "(2501000 - 2361 * " ~ t_evap_outlet_c ~ ")" %}
    {% set p_sat_in = "(610.78 * exp(17.27 * " ~ t_evap_inlet_c ~ " / (" ~ t_evap_inlet_c ~ " + 237.3)))" %}
    {% set p_sat_out = "(610.78 * exp(17.27 * " ~ t_evap_outlet_c ~ " / (" ~ t_evap_outlet_c ~ " + 237.3)))" %}
    {% set w_evap_in = "(0.622 * (" ~ rh_evap_inlet_pct ~ " / 100 * " ~ p_sat_in ~ ") / (" ~ pressure_pa ~ " - (" ~ rh_evap_inlet_pct ~ " / 100 * " ~ p_sat_in ~ ")))" %}
    {% set w_evap_out = "(0.622 * (" ~ rh_evap_outlet_pct ~ " / 100 * " ~ p_sat_out ~ ") / (" ~ pressure_pa ~ " - (" ~ rh_evap_outlet_pct ~ " / 100 * " ~ p_sat_out ~ ")))" %}
    {% set q_total_w = "(" ~ airflow_evap_kg_s ~ " * (1006.0 * (" ~ t_evap_inlet_c ~ " - " ~ t_evap_outlet_c ~ ") + " ~ h_fg ~ " * (" ~ w_evap_in ~ " - " ~ w_evap_out ~ ")))" %}
    
    safe_divide({{ q_total_w }}, {{ power_compressor_w }})

{%- endmacro %}