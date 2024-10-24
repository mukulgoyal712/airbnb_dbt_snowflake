{{ config(
    materialized='table',
    schema='staging'
) }}

with source as (
      select * from {{ source('airbnb', 'listings') }}
),
renamed as (
    select
        {{ adapter.quote("ID") }},
        {{ adapter.quote("NAME") }},
        {{ adapter.quote("HOST_ID") }},
        {{ adapter.quote("HOST_NAME") }},
        {{ adapter.quote("NEIGHBOURHOOD_GROUP") }},
        {{ adapter.quote("NEIGHBOURHOOD") }},
        {{ adapter.quote("LATITUDE") }},
        {{ adapter.quote("LONGITUDE") }},
        {{ adapter.quote("ROOM_TYPE") }},
        {{ adapter.quote("PRICE") }},
        {{ adapter.quote("MINIMUM_NIGHTS") }},
        {{ adapter.quote("NUMBER_OF_REVIEWS") }},
        {{ adapter.quote("LAST_REVIEW") }},
        {{ adapter.quote("REVIEWS_PER_MONTH") }},
        {{ adapter.quote("CALCULATED_HOST_LISTINGS_COUNT") }},
        {{ adapter.quote("AVAILABILITY_365") }},
        {{ adapter.quote("NUMBER_OF_REVIEWS_LTM") }},
        {{ adapter.quote("LICENSE") }}

    from source
)
select * from renamed
  