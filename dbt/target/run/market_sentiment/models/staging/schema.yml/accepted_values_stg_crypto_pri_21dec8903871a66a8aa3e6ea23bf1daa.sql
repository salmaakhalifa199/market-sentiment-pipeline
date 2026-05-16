select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        symbol as value_field,
        count(*) as n_records

    from default_staging.stg_crypto_prices
    group by symbol

)

select *
from all_values
where value_field not in (
    'BTCUSDT','ETHUSDT','SOLUSDT'
)



      
    ) dbt_internal_test