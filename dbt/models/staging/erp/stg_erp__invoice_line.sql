with source as (

    select * from {{ source('erp', 'erp_invoice_line') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by invoice_no, invoice_line_no
            order by _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast(nullif(trim(invoice_no), '') as varchar(20)) as invoice_no,
        cast(invoice_line_no as int) as invoice_line_no,
        cast(nullif(trim(document_type), '') as varchar(10)) as document_type,
        cast(case when document_type = 'CREDIT' then 1 else 0 end as bit) as is_credit,
        cast(nullif(trim(cast(invoice_date as varchar(30))), '') as date) as invoice_date,
        cast(nullif(trim(order_no), '') as varchar(20)) as order_no,
        cast(order_line_no as int) as order_line_no,
        cast(nullif(trim(customer_no), '') as varchar(20)) as customer_no,
        cast(nullif(trim(item_no), '') as varchar(20)) as item_no,
        -- negative on credit memos (returns)
        cast(quantity as int) as quantity,
        cast(unit_price as decimal(18,2)) as unit_price,
        cast(line_discount_pct as decimal(5,2)) as line_discount_pct,
        cast(line_amount as decimal(18,2)) as line_amount,
        cast(nullif(trim(shipment_no), '') as varchar(20)) as shipment_no,
        cast(posted_at as datetime2(6)) as posted_at,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
