with source as (

    select * from {{ source('dist', 'dist_depletion_badger') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by month_ending, retailer_id, product_code
            order by _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast('BADGER' as varchar(20)) as distributor_code,
        -- arrives as MM/DD/YYYY (month end); store the first day of the month like the other distributors
        cast(concat(right(trim(month_ending), 4), '-', left(trim(month_ending), 2), '-01') as date) as report_month,
        cast(nullif(trim(retailer_id), '') as varchar(40)) as distributor_account_id,
        cast(nullif(trim(retailer), '') as varchar(120)) as account_name,
        cast(nullif(trim(city), '') as varchar(60)) as city,
        cast(nullif(trim(state), '') as varchar(2)) as state_code,
        -- premise type is not in this distributor's file
        cast(null as varchar(20)) as premise_type,
        cast(nullif(trim(product_code), '') as varchar(40)) as distributor_sku,
        cast(nullif(trim(product), '') as varchar(120)) as product_description,
        cast(qty as int) as quantity,
        cast(case trim(uom) when 'KG' then 'KEG' when 'CS' then 'CASE' end as varchar(10)) as unit_type,
        _landed_date,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
