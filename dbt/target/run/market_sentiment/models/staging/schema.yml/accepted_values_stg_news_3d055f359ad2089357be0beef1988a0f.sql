select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        sentiment_strength as value_field,
        count(*) as n_records

    from default_staging.stg_news
    group by sentiment_strength

)

select *
from all_values
where value_field not in (
    'strong_positive','mild_positive','neutral','mild_negative','strong_negative'
)



      
    ) dbt_internal_test