
 -- depends_on: {{ ref('raw_date_dim') }}
{{
    config(
        materialized='incremental'
    )
}}
 
{%- set source_model = 'raw_date_dim' -%}
{%- set pk = 'DATE_HASH_KEY' -%}
{%- set nk = ['DATE','DAY'] -%}
{%- set ldts = 'LOAD_DATE' -%}


{{ dim_table(source_model,pk,nk,ldts) }}

