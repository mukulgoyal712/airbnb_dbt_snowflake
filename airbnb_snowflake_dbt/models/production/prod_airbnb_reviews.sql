{{ config
    (
    materialized='table',
    schema='prod',
    alias='prod_reviews'
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
  