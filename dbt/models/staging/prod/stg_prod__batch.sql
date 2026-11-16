with source as (

    select * from {{ source('prod', 'prod_batch') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by batch_id
            order by cast(updated_at as datetime2(6)) desc, _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast(nullif(trim(batch_id), '') as varchar(20)) as batch_id,
        cast(nullif(trim(brand_code), '') as varchar(10)) as brand_code,
        cast(nullif(trim(recipe_version), '') as varchar(10)) as recipe_version,
        cast(nullif(trim(cast(brew_date as varchar(30))), '') as date) as brew_date,
        cast(nullif(trim(tank_id), '') as varchar(10)) as tank_id,
        cast(planned_volume_bbl as decimal(9,2)) as planned_volume_bbl,
        cast(actual_volume_bbl as decimal(9,2)) as actual_volume_bbl,
        cast(nullif(trim(status), '') as varchar(20)) as batch_status,
        cast(nullif(trim(cast(package_date as varchar(30))), '') as date) as package_date,
        cast(nullif(trim(brewer), '') as varchar(60)) as brewer,
        cast(updated_at as datetime2(6)) as updated_at,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
