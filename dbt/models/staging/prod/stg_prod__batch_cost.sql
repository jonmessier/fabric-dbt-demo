with source as (

    select * from {{ source('prod', 'prod_batch_cost') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by batch_id, cost_type
            order by _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast(nullif(trim(batch_id), '') as varchar(20)) as batch_id,
        cast(nullif(trim(cost_type), '') as varchar(30)) as cost_type,
        cast(amount as decimal(18,2)) as cost_amount,
        cast(nullif(trim(cast(posted_date as varchar(30))), '') as date) as posted_date,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
