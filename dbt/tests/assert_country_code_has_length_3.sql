select 
    country_code
from {{ ref('country_codes') }}
where LEN(country_code) <> 3