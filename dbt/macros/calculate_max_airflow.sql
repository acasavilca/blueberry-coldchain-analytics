{% macro max_airflow_evap_kg_s(q_rated_w, td_design, bf) %}
    ({{ q_rated_w }}/{{ td_design }}) / (1006.0*(1 - {{ bf }}))
{%- endmacro %}
