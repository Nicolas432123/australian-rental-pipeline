with suburbs as (

    select distinct
        suburb,
        state
    from {{ ref('stg_rentals_clean') }}

)

select
    row_number() over (order by state, suburb) as suburb_id,
    suburb,
    state
from suburbs