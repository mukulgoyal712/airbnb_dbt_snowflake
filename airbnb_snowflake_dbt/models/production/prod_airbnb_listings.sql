{{ config
    (
    materialized='table',
    schema='prod',
    alias = 'prod_listings'
    ) 
}}

with source as (
      select * from {{ source('airbnb', 'listings') }}
),
renamed as (
    select
        ID::NUMBER AS ID,
        NAME,
        HOST_ID::NUMBER AS HOST_ID,
        HOST_NAME,
        -- NEIGHBOURHOOD_GROUP,
        NEIGHBOURHOOD,
        LATITUDE::DECIMAL(10,2) AS LATITUDE,
        LONGITUDE::DECIMAL(10,2) AS LONGITUDE,
        ROOM_TYPE,
        (CASE WHEN PRICE::DECIMAL IS NULL THEN 0 ELSE PRICE::DECIMAL END) AS PRICE,
        (CASE WHEN MINIMUM_NIGHTS::NUMBER IS NULL THEN 0 ELSE MINIMUM_NIGHTS::NUMBER END) AS MINIMUM_NIGHTS,
        (CASE WHEN NUMBER_OF_REVIEWS::NUMBER IS NULL THEN 0 ELSE NUMBER_OF_REVIEWS::NUMBER END) AS NUMBER_OF_REVIEWS,
        LAST_REVIEW::DATE AS LAST_REVIEW,
        REVIEWS_PER_MONTH::DECIMAL AS REVIEWS_PER_MONTH,
        -- CALCULATED_HOST_LISTINGS_COUNT::NUMBER,
        AVAILABILITY_365,
        NUMBER_OF_REVIEWS_LTM,
        CASE WHEN LICENSE ILIKE '%exempt%' then 'Exempt' ELSE LICENSE END AS LICENSE 
    from source
)
select * from renamed
  