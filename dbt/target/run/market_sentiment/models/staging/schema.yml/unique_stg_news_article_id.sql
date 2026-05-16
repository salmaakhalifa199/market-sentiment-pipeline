select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    article_id as unique_field,
    count(*) as n_records

from default_staging.stg_news
where article_id is not null
group by article_id
having count(*) > 1



      
    ) dbt_internal_test