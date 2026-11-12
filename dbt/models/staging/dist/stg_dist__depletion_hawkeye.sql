with source as (

    select * from {{ source('dist', 'dist_depletion_hawkeye') }}

),

-- bronze is append-only, so the same key can arrive more than once; keep the newest version
ranked as (

    select
        *,
        row_number() over (
            partition by rpt_yyyymm, outlet_cd, brand_pkg
            order by _ingested_at desc
        ) as _row_rank
    from source

),

renamed as (

    select
        cast('HAWKEYE' as varchar(20)) as distributor_code,
        cast(concat(left(trim(cast(rpt_yyyymm as varchar(6))), 4), '-', right(trim(cast(rpt_yyyymm as varchar(6))), 2), '-01') as date) as report_month,
        cast(nullif(trim(cast(outlet_cd as varchar(40))), '') as varchar(40)) as distributor_account_id,
        cast(nullif(trim(outlet_nm), '') as varchar(120)) as account_name,
        cast(nullif(trim(outlet_city), '') as varchar(60)) as city,
        -- single-state distributor; the file has no state column
        cast('IA' as varchar(2)) as state_code,
        cast(case trim(premise) when 'ON-PREM' then 'ON_PREMISE' when 'OFF-PREM' then 'OFF_PREMISE' end as varchar(20)) as premise_type,
        cast(nullif(trim(cast(brand_pkg as varchar(40))), '') as varchar(40)) as distributor_sku,
        cast(nullif(trim(pkg_desc), '') as varchar(120)) as product_description,
        cast(units_sold as int) as quantity,
        cast(case when pkg_desc like '%keg%' then 'KEG' else 'CASE' end as varchar(10)) as unit_type,
        _landed_date,
        _source_file,
        _ingested_at
    from ranked
    where _row_rank = 1

)

select * from renamed
