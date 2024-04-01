with data as (
    select * from {{ ref('stg_data') }}
)

select 
    fecha,
    linea,
    estacion, 
    sum(pax_total) as total_pax
from data 
group by fecha, linea, estacion
order by fecha ASC