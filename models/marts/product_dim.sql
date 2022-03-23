{{
    config(
        materialized='incremental'
    )
}}


with temp as (
    SELECT 
    {{ dbt_utils.surrogate_key('PRODUCT','CATEGORY') }} as PRODUCT_HASH_KEY,
    PRODUCT,
    CATEGORY,
    GETDATE() AS LOAD_DATE
    

    from {{ ref('raw_product_dim')}}

)
,
rank_1 as (

    SELECT
    PRODUCT_HASH_KEY,
    PRODUCT,
    CATEGORY,
    LOAD_DATE,
    ROW_NUMBER() OVER ( PARTITION BY PRODUCT_HASH_KEY 
                            ORDER BY LOAD_DATE ) as row_number
    FROM  temp
    WHERE PRODUCT_HASH_KEY IS NOT NULL
    QUALIFY row_number=1                       

),
record_to_insert as (

    SELECT d.PRODUCT_HASH_KEY,
           d.PRODUCT,
           d.CATEGORY,
           d.LOAD_DATE
    FROM rank_1 as d
    {% if is_incremental() %}


    LEFT JOIN {{ this }} as t
    ON t.PRODUCT_HASH_KEY= d.PRODUCT_HASH_KEY
    WHERE t.PRODUCT_HASH_KEY IS NULL
        
    {% endif %}        
)

SELECT * FROM record_to_insert