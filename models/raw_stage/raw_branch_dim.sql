SELECT 
    BRANCH,
    CITY,
    GETDATE() AS LOAD_DATE

    FROM {{source('BHATBHATENI_MODELING','BHATBHATENI_STAGE_TABLE') }}
    