with source as (

    select * from {{ source('erp', 'erp_sales_line') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by order_no, line_no
            order by _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast(nullif(trim(order_no), '') as varchar(20)) as order_no,
        cast(line_no as int) as order_line_no,
        cast(nullif(trim(item_no), '') as varchar(20)) as item_no,
        cast(quantity as int) as quantity,
        cast(nullif(trim(unit_of_measure), '') as varchar(10)) as unit_of_measure,
        cast(unit_price as decimal(18,2)) as unit_price,
        cast(line_discount_pct as decimal(5,2)) as line_discount_pct,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
