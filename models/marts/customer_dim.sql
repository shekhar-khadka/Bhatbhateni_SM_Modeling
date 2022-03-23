{{
    config(
        materialized='incremental'
    )
}}


with temp as (
    SELECT 
    {{ dbt_utils.surrogate_key('CUSTOMER_NAME','AGE','CONTACT') }} as CUSTOMER_HASH_KEY,
    CUSTOMER_NAME,
    AGE,
    CONTACT,
    GETDATE() AS LOAD_DATE
    

    from {{ ref('raw_customers_dim')}}

)
,
rank_1 as (

    SELECT
    CUSTOMER_HASH_KEY,
    CUSTOMER_NAME,
    AGE,
    CONTACT,
    LOAD_DATE,
    ROW_NUMBER() OVER ( PARTITION BY CUSTOMER_HASH_KEY 
                            ORDER BY LOAD_DATE ) as row_number
    FROM  temp
    WHERE CUSTOMER_HASH_KEY IS NOT NULL
    QUALIFY row_number=1                       

),
record_to_insert as (

    SELECT d.CUSTOMER_HASH_KEY,
           d.CUSTOMER_NAME,
           d.AGE,
           d.CONTACT,
           d.LOAD_DATE
    FROM rank_1 as d
    {% if is_incremental() %}


    LEFT JOIN {{ this }} as t
    ON t.CUSTOMER_HASH_KEY= d.CUSTOMER_HASH_KEY
    WHERE t.CUSTOMER_HASH_KEY IS NULL
        
    {% endif %}        
)

SELECT * FROM record_to_insert