
    
    

with all_values as (

    select
        final_sentiment as value_field,
        count(*) as n_records

    from default_staging.stg_sentiment
    group by final_sentiment

)

select *
from all_values
where value_field not in (
    'bullish','bearish','neutral'
)


