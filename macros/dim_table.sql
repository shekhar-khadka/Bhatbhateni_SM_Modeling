{% macro dim_table(source_model,pk,nk,ldts) %}

with temp AS (

{{- temp_gen(pk,nk,ldts,source_model,indent=4) }}
),

row_rank_1 AS (
    
    
    {{- rank_parser(pk,nk,ldts,indent=4) }}
),

records_to_insert AS (
    {% set prefix='a' %}
    {{- records_to_insert_parser(pk,nk,ldts,prefix, indent=4) }}
    {% if is_incremental() %}
        LEFT JOIN {{ this }} AS t
        {{ parse_incremental(pk,prefix=['a','t'], indent=4) }}
    {% endif %}
)

SELECT * FROM records_to_insert

{% endmacro %}

