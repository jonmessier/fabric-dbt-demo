with source as (

    select * from {{ source('erp', 'erp_shipment') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by shipment_no
            order by _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast(nullif(trim(shipment_no), '') as varchar(20)) as shipment_no,
        cast(nullif(trim(order_no), '') as varchar(20)) as order_no,
        cast(nullif(trim(customer_no), '') as varchar(20)) as customer_no,
        cast(nullif(trim(cast(ship_date as varchar(30))), '') as date) as ship_date,
        cast(nullif(trim(ship_from), '') as varchar(30)) as ship_from,
        cast(nullif(trim(carrier), '') as varchar(40)) as carrier,
        cast(posted_at as datetime2(6)) as posted_at,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
