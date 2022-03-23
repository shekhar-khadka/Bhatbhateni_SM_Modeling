{{
    config(
        materialized='incremental'
    )
}}


with temp as (
    SELECT 
    {{ dbt_utils.surrogate_key('CUSTOMER_NAME','AGE','CONTACT') }} as CUSTOMER_HASH_KEY,
    {{ dbt_utils.surrogate_key('PRODUCT','CATEGORY') }} as PRODUCT_HASH_KEY,
    {{ dbt_utils.surrogate_key('EMPLOYEE','DESIGNATION') }} as EMPLOYEE_HASH_KEY,
    {{ dbt_utils.surrogate_key('DATE','DAY') }} as DATE_HASH_KEY,
    {{ dbt_utils.surrogate_key('BRANCH','CITY') }} as BRANCH_HASH_KEY,
    QUANTITY,
    SALES_AMOUNT,

    {{ dbt_utils.surrogate_key('CUSTOMER_HASH_KEY','PRODUCT_HASH_KEY','EMPLOYEE_HASH_KEY','DATE_HASH_KEY','BRANCH_HASH_KEY','QUANTITY','SALES_AMOUNT') }} as SALES_HASH_KEY,


    GETDATE() AS LOAD_DATE
    

    from {{ ref('raw_sales_fact')}}

)
,
rank_1 as (

    SELECT
    SALES_HASH_KEY,
    CUSTOMER_HASH_KEY,
    PRODUCT_HASH_KEY,
    EMPLOYEE_HASH_KEY,
    DATE_HASH_KEY,
    BRANCH_HASH_KEY,
    QUANTITY,
    SALES_AMOUNT,

    LOAD_DATE,
    ROW_NUMBER() OVER ( PARTITION BY SALES_HASH_KEY 
                            ORDER BY LOAD_DATE ) as row_number
    FROM  temp
    WHERE SALES_HASH_KEY IS NOT NULL
    QUALIFY row_number=1                       

),
record_to_insert as (

    SELECT  d.SALES_HASH_KEY,
            d.CUSTOMER_HASH_KEY,
            d.PRODUCT_HASH_KEY,
            d.EMPLOYEE_HASH_KEY,
            d.DATE_HASH_KEY,
            d.BRANCH_HASH_KEY,
            d.QUANTITY,
            d.SALES_AMOUNT,

            d.LOAD_DATE
    FROM rank_1 as d
    {% if is_incremental() %}


    LEFT JOIN {{ this }} as t
    ON t.SALES_HASH_KEY= d.SALES_HASH_KEY
    WHERE t.SALES_HASH_KEY IS NULL
        
    {% endif %}        
)

SELECT * FROM record_to_insert