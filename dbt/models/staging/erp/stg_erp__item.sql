with source as (

    select * from {{ source('erp', 'erp_item') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by item_no
            order by cast(updated_at as datetime2(6)) desc, _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast(nullif(trim(item_no), '') as varchar(20)) as item_no,
        cast(nullif(trim(description), '') as varchar(120)) as item_description,
        cast(nullif(trim(brand_code), '') as varchar(10)) as brand_code,
        cast(nullif(trim(brand_name), '') as varchar(80)) as brand_name,
        cast(nullif(trim(style), '') as varchar(40)) as style,
        cast(nullif(trim(category), '') as varchar(20)) as product_category,
        cast(abv as decimal(4,1)) as abv,
        cast(nullif(trim(package_code), '') as varchar(20)) as package_code,
        cast(nullif(trim(base_uom), '') as varchar(10)) as base_uom,
        cast(standard_cost as decimal(18,2)) as standard_cost,
        cast(nullif(trim(status), '') as varchar(20)) as item_status,
        cast(nullif(trim(cast(created_date as varchar(30))), '') as date) as created_date,
        cast(updated_at as datetime2(6)) as updated_at,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
