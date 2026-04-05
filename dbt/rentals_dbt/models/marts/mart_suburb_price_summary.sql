{{ config(materialized='table') }}

select
    s.suburb,
    s.state,
    count(*) as total_listings,
    avg(f.price_aud) as avg_price,
    min(f.price_aud) as min_price,
    max(f.price_aud) as max_price
from {{ ref('fct_rental_listings') }} f
join {{ ref('dim_suburbs') }} s
    on f.suburb_id = s.suburb_id
group by
    s.suburb,
    s.state