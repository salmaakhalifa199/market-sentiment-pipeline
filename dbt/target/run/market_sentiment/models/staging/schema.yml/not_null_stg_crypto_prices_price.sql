select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select price
from default_staging.stg_crypto_prices
where price is null



      
    ) dbt_internal_test