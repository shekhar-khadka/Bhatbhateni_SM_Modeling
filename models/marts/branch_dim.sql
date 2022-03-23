{{
    config(
        materialized='incremental'
    )
}}


with temp as (
    SELECT 
    {{ dbt_utils.surrogate_key('BRANCH','CITY') }} as BRANCH_HASH_KEY,
    BRANCH,
    CITY,
    GETDATE() AS LOAD_DATE
    

    from {{ ref('raw_branch_dim')}}

)
,
rank_1 as (

    SELECT
    BRANCH_HASH_KEY,
    BRANCH,
    CITY,
    LOAD_DATE,
    ROW_NUMBER() OVER ( PARTITION BY BRANCH_HASH_KEY 
                            ORDER BY LOAD_DATE ) as row_number
    FROM  temp
    WHERE BRANCH_HASH_KEY IS NOT NULL
    QUALIFY row_number=1                       

),
record_to_insert as (

    SELECT d.BRANCH_HASH_KEY,
           d.BRANCH,
           d.CITY,
           d.LOAD_DATE
    FROM rank_1 as d
    {% if is_incremental() %}


    LEFT JOIN {{ this }} as t
    ON t.BRANCH_HASH_KEY= d.BRANCH_HASH_KEY
    WHERE t.BRANCH_HASH_KEY IS NULL
        
    {% endif %}        
)

SELECT * FROM record_to_insert