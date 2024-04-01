with 

source as (

    select * from {{ source('staging', 'data') }}

),

renamed as (

select
    {{ dbt_utils.generate_surrogate_key(['fecha_desde', 'fecha_hasta']) }} as periodoId,
    fecha,
    desde,
    hasta,
    linea,
    molinete,
    estacion,
    pax_pagos,
    pax_pases_pagos,
    pax_franq,
    pax_total,
    fecha_desde,
    fecha_hasta
from source

)

select * from renamed
