{% macro temp_gen(pk,nk,ldts,source_model,indent=4)  %}
     {% set hash_key = dbt_utils.surrogate_key(nk)+' as '+pk  %}

     {% set payloads= nk+[hash_key]+[ldts] %}
     
     SELECT 
     {% for col in payloads %}
       {{- col | indent(4) }}{{',' if not loop.last }}
      
    {% endfor %}
    FROM {{ ref(source_model) }}

{% endmacro %}

{%- macro rank_parser(pk,nk,ldts,prefix,indent=4) -%}
    {% set payloads= nk+[pk]+[ldts] %}
     
     SELECT 
     {% for col in payloads %}
       {{- col | indent(4) }},
      
      {% endfor %}
    
    ROW_NUMBER() OVER(
               PARTITION BY {{ pk }}
               ORDER BY {{ ldts }}
           ) AS row_number
    FROM temp 
    WHERE {{ pk }} IS NOT NULL
    QUALIFY row_number = 1
{% endmacro %}   

{% macro records_to_insert_parser(pk,nk,ldts,prefix, indent=4) %}
    SELECT {{prefix}}.{{ pk }},
    {% for cols_name in nk -%}
        {{prefix}}.{{- cols_name | indent(4) -}}{{ ",\n    " }}
    {%- endfor %}
    {{- prefix}}.{{ ldts }}
    FROM row_rank_1 as {{ prefix}}

{% endmacro %}


{% macro parse_incremental(pk,prefix, indent=4) %}
     
    ON {{prefix[0]}}.{{ pk }} = {{prefix[1]}}.{{ pk }}
    WHERE {{prefix[1]}}.{{ pk }} IS NULL

{% endmacro %}