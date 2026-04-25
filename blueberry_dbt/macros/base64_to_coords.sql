{% macro base64_to_coords(base_64_key) %}
    SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64({{ base64_key }}))
{%- endmacro %}