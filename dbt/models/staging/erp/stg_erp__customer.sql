with source as (

    select * from {{ source('erp', 'erp_customer') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by customer_no
            order by cast(updated_at as datetime2(6)) desc, _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast(nullif(trim(customer_no), '') as varchar(20)) as customer_no,
        cast(nullif(trim(customer_name), '') as varchar(120)) as customer_name,
        cast(nullif(trim(customer_type), '') as varchar(20)) as customer_type,
        cast(nullif(trim(address_line1), '') as varchar(120)) as address_line1,
        cast(nullif(trim(city), '') as varchar(60)) as city,
        cast(state as varchar(40)) as state_raw,
        cast(upper(trim(state)) as varchar(40)) as state_text,
        cast(nullif(trim(postal_code), '') as varchar(10)) as postal_code,
        cast(nullif(trim(price_tier), '') as varchar(5)) as price_tier,
        cast(nullif(trim(territory), '') as varchar(30)) as territory,
        cast(nullif(trim(salesperson), '') as varchar(60)) as salesperson,
        cast(nullif(trim(payment_terms), '') as varchar(20)) as payment_terms,
        cast(nullif(trim(cast(created_date as varchar(30))), '') as date) as created_date,
        cast(updated_at as datetime2(6)) as updated_at,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
