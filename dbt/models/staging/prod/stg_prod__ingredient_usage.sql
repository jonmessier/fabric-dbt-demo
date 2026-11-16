with source as (

    select * from {{ source('prod', 'prod_ingredient_usage') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by batch_id, ingredient
            order by _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast(nullif(trim(batch_id), '') as varchar(20)) as batch_id,
        cast(nullif(trim(ingredient), '') as varchar(60)) as ingredient,
        cast(quantity as decimal(18,4)) as quantity,
        cast(nullif(trim(uom), '') as varchar(10)) as uom,
        cast(cost as decimal(18,2)) as ingredient_cost,
        cast(nullif(trim(lot_no), '') as varchar(20)) as lot_no,
        cast(recorded_at as datetime2(6)) as recorded_at,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
