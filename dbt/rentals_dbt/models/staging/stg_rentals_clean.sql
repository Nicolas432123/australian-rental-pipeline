with source as (

    select *
    from {{ source('rentals_raw', 'rentals_clean') }}

)

select
    listing_id,
    price_aud,
    property_type,
    suburb,
    state,
    postcode,
    latitude,
    longitude,
    bedrooms,
    bathrooms,
    parking_spaces,
    agency_name,
    amenities,
    loaded_at

from source