with source as (

    select * from {{ source('pos', 'pos_line_item') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by transaction_id, line_id
            order by _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast(nullif(trim(transaction_id), '') as varchar(30)) as transaction_id,
        cast(line_id as int) as line_id,
        cast(nullif(trim(item_code), '') as varchar(30)) as item_code,
        cast(nullif(trim(item_name), '') as varchar(80)) as item_name,
        cast(nullif(trim(category), '') as varchar(20)) as sales_category,
        cast(nullif(trim(variation), '') as varchar(20)) as variation,
        cast(case
            when item_code like 'DRAFT-%' then substring(item_code, 7, 3)
            when item_code like 'TOGO-%' then substring(item_code, 6, 3)
        end as varchar(10)) as brand_code,
        -- serving size -> package_code, so volume converts through ref_uom_conversion like every other sale
        cast(case
            when item_code like 'DRAFT-%' and variation = '16 oz' then 'POUR-16'
            when item_code like 'DRAFT-%' and variation = '10 oz' then 'POUR-10'
            when item_code like 'DRAFT-%' and variation = 'Flight' then 'FLIGHT'
            when item_code like 'TOGO-%4x16' then 'PK-4x16'
            when item_code like 'TOGO-%6x12' then 'PK-6x12'
        end as varchar(20)) as package_code,
        cast(quantity as int) as quantity,
        cast(unit_price as decimal(18,2)) as unit_price,
        cast(discount_amount as decimal(18,2)) as discount_amount,
        cast(line_total as decimal(18,2)) as line_amount,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
