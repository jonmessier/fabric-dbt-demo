with source as (

    select * from {{ source('erp', 'erp_price_list') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by item_no, price_tier, starting_date
            order by cast(updated_at as datetime2(6)) desc, _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast(nullif(trim(item_no), '') as varchar(20)) as item_no,
        cast(nullif(trim(price_tier), '') as varchar(5)) as price_tier,
        cast(unit_price as decimal(18,2)) as list_price,
        cast(nullif(trim(currency), '') as varchar(3)) as currency,
        cast(nullif(trim(cast(starting_date as varchar(30))), '') as date) as starting_date,
        cast(nullif(trim(cast(ending_date as varchar(30))), '') as date) as ending_date,
        cast(case when nullif(trim(cast(ending_date as varchar(30))), '') is null then 1 else 0 end as bit) as is_current,
        cast(updated_at as datetime2(6)) as updated_at,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
