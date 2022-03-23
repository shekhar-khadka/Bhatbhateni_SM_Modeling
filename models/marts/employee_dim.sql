{{
    config(
        materialized='incremental'
    )
}}


with temp as (
    SELECT 
    {{ dbt_utils.surrogate_key('EMPLOYEE','DESIGNATION') }} as EMPLOYEE_HASH_KEY,
    EMPLOYEE,
    DESIGNATION,
    GETDATE() AS LOAD_DATE
    

    from {{ ref('raw_employee_dim')}}

)
,
rank_1 as (

    SELECT
    EMPLOYEE_HASH_KEY,
    EMPLOYEE,
    DESIGNATION,
    LOAD_DATE,
    ROW_NUMBER() OVER ( PARTITION BY EMPLOYEE_HASH_KEY 
                            ORDER BY LOAD_DATE ) as row_number
    FROM  temp
    WHERE EMPLOYEE_HASH_KEY IS NOT NULL
    QUALIFY row_number=1                       

),
record_to_insert as (

    SELECT d.EMPLOYEE_HASH_KEY,
           d.EMPLOYEE,
           d.DESIGNATION,
           d.LOAD_DATE
    FROM rank_1 as d
    {% if is_incremental() %}


    LEFT JOIN {{ this }} as t
    ON t.EMPLOYEE_HASH_KEY= d.EMPLOYEE_HASH_KEY
    WHERE t.EMPLOYEE_HASH_KEY IS NULL
        
    {% endif %}        
)

SELECT * FROM record_to_insert