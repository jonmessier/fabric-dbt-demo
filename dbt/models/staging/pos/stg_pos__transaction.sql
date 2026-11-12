-- A re-sent POS file lands the same transactions twice in bronze. One row per transaction_id survives.
with source as (

    select * from {{ source('pos', 'pos_transaction') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by transaction_id
            order by _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast(nullif(trim(transaction_id), '') as varchar(30)) as transaction_id,
        cast(created_at as datetime2(6)) as transaction_at,
        cast(cast(created_at as datetime2(6)) as date) as transaction_date,
        cast(nullif(trim(location), '') as varchar(20)) as location,
        cast(nullif(trim(register), '') as varchar(10)) as register_id,
        cast(nullif(trim(employee), '') as varchar(60)) as employee,
        cast(nullif(trim(status), '') as varchar(20)) as transaction_status,
        cast(nullif(trim(payment_type), '') as varchar(20)) as payment_type,
        cast(nullif(trim(customer_program), '') as varchar(20)) as customer_program,
        cast(subtotal as decimal(18,2)) as subtotal_amount,
        cast(tax as decimal(18,2)) as tax_amount,
        cast(tip as decimal(18,2)) as tip_amount,
        cast(total as decimal(18,2)) as total_amount,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
