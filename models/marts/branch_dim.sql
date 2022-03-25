 -- depends_on: {{ ref('raw_branch_dim') }}
{{
    config(
        materialized='incremental'
    )
}}
 
{%- set source_model = "raw_branch_dim" -%}
{%- set pk = "BRANCH_HASH_KEY" -%}
{%- set nk = ["BRANCH","CITY"] -%}
{%- set ldts = "LOAD_DATE" -%}


{{ dim_table(source_model,pk,nk,ldts) }}