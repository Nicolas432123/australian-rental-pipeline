{{ config(materialized='table') }}

with listings as (

    select
        listing_id,
        suburb,
        state,
        property_type,
        postcode,
        latitude,
        longitude,
        price_aud,
        bedrooms,
        bathrooms,
        parking_spaces,
        agency_name,
        amenities,
        loaded_at
    from {{ ref('stg_rentals_clean') }}

),

suburbs as (

    select
        suburb_id,
        suburb,
        state
    from {{ ref('dim_suburbs') }}

),

property_types as (

    select
        property_type_id,
        property_type
    from {{ ref('dim_property_types') }}

)

select
    l.listing_id,
    s.suburb_id,
    p.property_type_id,
    l.postcode,
    l.latitude,
    l.longitude,
    l.price_aud,
    l.bedrooms,
    l.bathrooms,
    l.parking_spaces,
    l.agency_name,
    l.amenities,
    l.loaded_at
from listings l
left join suburbs s
    on l.suburb = s.suburb
   and l.state = s.state
left join property_types p
    on l.property_type = p.property_type