select * 
from {{ref('prod_airbnb_listings')}}
where PRICE is null or price<0