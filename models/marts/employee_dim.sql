
 -- depends_on: {{ ref('raw_employee_dim') }}
{{
    config(
        materialized='incremental'
    )
}}
 
{%- set source_model = 'raw_employee_dim' -%}
{%- set pk = 'EMPLOYEE_HASH_KEY' -%}
{%- set nk = ['EMPLOYEE','DESIGNATION'] -%}
{%- set ldts = 'LOAD_DATE' -%}


{{ dim_table(source_model,pk,nk,ldts) }}


