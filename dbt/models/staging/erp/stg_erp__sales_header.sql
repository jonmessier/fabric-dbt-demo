with source as (

    select * from {{ source('erp', 'erp_sales_header') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by order_no
            order by cast(updated_at as datetime2(6)) desc, _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast(nullif(trim(order_no), '') as varchar(20)) as order_no,
        cast(nullif(trim(customer_no), '') as varchar(20)) as customer_no,
        cast(nullif(trim(cast(order_date as varchar(30))), '') as date) as order_date,
        cast(nullif(trim(cast(requested_ship_date as varchar(30))), '') as date) as requested_ship_date,
        cast(nullif(trim(status), '') as varchar(20)) as order_status,
        cast(nullif(trim(order_source), '') as varchar(20)) as order_source,
        cast(updated_at as datetime2(6)) as updated_at,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
