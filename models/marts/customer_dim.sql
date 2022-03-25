

 -- depends_on: {{ ref('raw_customers_dim') }}
{{
    config(
        materialized='incremental'
    )
}}
 
{%- set source_model = 'raw_customers_dim' -%}
{%- set pk = 'CUSTOMER_HASH_KEY' -%}
{%- set nk = ['CUSTOMER_NAME','AGE','CONTACT'] -%}
{%- set ldts = 'LOAD_DATE' -%}


{{ dim_table(source_model,pk,nk,ldts) }}
