{% macro mask_ip(ip_column) %}
    CASE 
        WHEN {{ ip_column }} IS NOT NULL 
        THEN REGEXP_REPLACE({{ ip_column }}, '\.\d+$', '.***')
        ELSE '0.0.0.0'
    END
{% endmacro %}