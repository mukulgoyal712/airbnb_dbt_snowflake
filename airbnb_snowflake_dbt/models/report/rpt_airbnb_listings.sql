SELECT HOST_ID
       ,HOST_NAME
       ,NEIGHBOURHOOD
       ,{{calculate_average('PRICE')}} AS AVG_PRICE
    --    ,ROUND(AVG(PRICE),2) AS AVG_PRICE
       ,{{calculate_average('PRICE')}}*{{calculate_average('MINIMUM_NIGHTS')}} AS AVG_REVENUE
       ,{{calculate_average('MINIMUM_NIGHTS')}} AS AVG_MIN_NIGHTS
    --    ,ROUND(AVG(MINIMUM_NIGHTS),0) AS AVG_MIN_NIGHTS
       ,{{calculate_sum('NUMBER_OF_REVIEWS')}} AS TOTAL_REVIEWS
    --    ,SUM(NUMBER_OF_REVIEWS) AS TOTAL_REVIEWS
       ,CASE WHEN LICENSE IS NOT NULL THEN 'Y' ELSE 'N' END AS LICENSE_FLG

FROM {{ref('prod_airbnb_listings')}}
GROUP BY HOST_ID
       ,HOST_NAME
       ,NEIGHBOURHOOD
       ,CASE WHEN LICENSE IS NOT NULL THEN 'Y' ELSE 'N' END