{{ config
    (
    materialized='table',
    schema='prod',
    alias='prod_reviews' --to name different then sql file name
    ) 
}}

with source as (
      select * from {{ source('airbnb', 'reviews') }}
),
renamed as (
    select
        {{ adapter.quote("LISTING_ID") }},
        {{ adapter.quote("DATE") }}

    from source
)
select * from renamed
  