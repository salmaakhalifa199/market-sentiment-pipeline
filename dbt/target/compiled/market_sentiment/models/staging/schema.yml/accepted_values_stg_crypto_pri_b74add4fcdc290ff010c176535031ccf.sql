
    
    

with all_values as (

    select
        price_direction as value_field,
        count(*) as n_records

    from default_staging.stg_crypto_prices
    group by price_direction

)

select *
from all_values
where value_field not in (
    'bullish','bearish','neutral'
)


