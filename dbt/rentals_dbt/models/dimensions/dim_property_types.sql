with property_types as (

    select distinct
        property_type
    from {{ ref('stg_rentals_clean') }}
    where property_type is not null

)

select
    row_number() over (order by property_type) as property_type_id,
    property_type
from property_types