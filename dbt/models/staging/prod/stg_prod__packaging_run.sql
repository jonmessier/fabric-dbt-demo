with source as (

    select * from {{ source('prod', 'prod_packaging_run') }}

),

-- bronze is append-only, so the same key can arrive more than once (a corrected run is re-sent with the
-- same run_id); keep the newest version: latest recorded_at, then latest load
ranked as (

    select
        *,
        row_number() over (
            partition by run_id
            order by cast(recorded_at as datetime2(6)) desc, _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast(nullif(trim(run_id), '') as varchar(30)) as run_id,
        cast(nullif(trim(batch_id), '') as varchar(20)) as batch_id,
        cast(nullif(trim(item_no), '') as varchar(20)) as item_no,
        cast(nullif(trim(cast(run_date as varchar(30))), '') as date) as run_date,
        cast(nullif(trim(line), '') as varchar(20)) as packaging_line,
        cast(units_packaged as int) as units_packaged,
        cast(volume_bbl as decimal(9,2)) as packaged_volume_bbl,
        cast(nullif(trim(operator), '') as varchar(60)) as operator,
        cast(recorded_at as datetime2(6)) as recorded_at,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
