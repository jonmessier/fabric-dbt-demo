with source as (

    select * from {{ source('dist', 'dist_depletion_northland') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by period, acct, supplier_item
            order by _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast('NORTHLAND' as varchar(20)) as distributor_code,
        cast(concat(trim(period), '-01') as date) as report_month,
        cast(nullif(trim(acct), '') as varchar(40)) as distributor_account_id,
        cast(nullif(trim(account_name), '') as varchar(120)) as account_name,
        cast(nullif(trim(city), '') as varchar(60)) as city,
        cast(nullif(trim(st), '') as varchar(2)) as state_code,
        cast(case trim(premise) when 'ON' then 'ON_PREMISE' when 'OFF' then 'OFF_PREMISE' end as varchar(20)) as premise_type,
        cast(nullif(trim(supplier_item), '') as varchar(40)) as distributor_sku,
        cast(nullif(trim(item_desc), '') as varchar(120)) as product_description,
        -- this layout has one column per unit type; a row only ever fills one of them
        cast(cast(cases as int) + cast(kegs as int) as int) as quantity,
        cast(case when cast(kegs as int) <> 0 then 'KEG' else 'CASE' end as varchar(10)) as unit_type,
        _landed_date,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
