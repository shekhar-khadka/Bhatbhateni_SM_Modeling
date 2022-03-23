{{
    config(
        materialized='incremental'
    )
}}


with temp as (
    SELECT 
    {{ dbt_utils.surrogate_key('DATE','DAY') }} as DATE_HASH_KEY,
    DATE,
    DAY,
    GETDATE() AS LOAD_DATE
    

    from {{ ref('raw_date_dim')}}

)
,
rank_1 as (

    SELECT
    DATE_HASH_KEY,
    DATE,
    DAY,
    LOAD_DATE,
    ROW_NUMBER() OVER ( PARTITION BY DATE_HASH_KEY 
                            ORDER BY LOAD_DATE ) as row_number
    FROM  temp
    WHERE DATE_HASH_KEY IS NOT NULL
    QUALIFY row_number=1                       

),
record_to_insert as (

    SELECT d.DATE_HASH_KEY,
           d.DATE,
           d.DAY,
           d.LOAD_DATE
    FROM rank_1 as d
    {% if is_incremental() %}


    LEFT JOIN {{ this }} as t
    ON t.DATE_HASH_KEY= d.DATE_HASH_KEY
    WHERE t.DATE_HASH_KEY IS NULL
        
    {% endif %}        
)

SELECT * FROM record_to_insert