select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select influence_score
from default_staging.stg_sentiment
where influence_score is null



      
    ) dbt_internal_test