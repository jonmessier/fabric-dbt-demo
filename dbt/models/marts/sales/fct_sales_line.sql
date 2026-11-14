{{
    config(
        materialized='incremental',
        unique_key='sales_line_key',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}

-- One row per sale line from the ERP and the taproom, with cost and gross margin.
--
-- Incremental: each run only processes lines loaded into bronze in the last 3 days before the
-- newest load already in this table, and replaces them by sales_line_key. The 3-day overlap picks
-- up late corrections without rebuilding 18 months of history. --full-refresh rebuilds everything.
--
-- Cost: ERP lines use the item's standard cost; taproom pours use the brand's standard cost per
-- barrel. Merchandise has no cost here, so no margin.

with

{% if is_incremental() %}
-- The cutoff is its own CTE, joined below, rather than a subquery in the where clause: Fabric
-- cannot resolve {{ this }} from a subquery when dbt nests the model in CTEs (unit tests, contracts).
-- An empty table (created with dbt run --empty in a new environment) loads everything.
incremental_cutoff as (

    select coalesce(dateadd(day, -3, max(_ingested_at)), cast('1900-01-01' as datetime2(6))) as cutoff
    from {{ this }}

),
{% endif %}

sales as (

    select unioned.*
    from {{ ref('int_sales_lines_unioned') }} as unioned
    {% if is_incremental() %}
    inner join incremental_cutoff
        on unioned._ingested_at > incremental_cutoff.cutoff
    {% endif %}

),

customer_history as (

    select * from {{ ref('snap_erp_customer') }}

),

brands as (

    select * from {{ ref('dim_brand') }}

),

costed as (

    select
        sales.*,
        case
            when sales.source_system = 'POS' then 'TAPROOM'
            else customer_history.dbt_scd_id
        end as customer_version_key,
        coalesce(
            sales.standard_cost_amount,
            sales.barrels * brands.standard_cost_per_bbl
        ) as cost_amount
    from sales
    left join customer_history
        on sales.customer_no = customer_history.customer_no
        and sales.sale_date >= cast(customer_history.dbt_valid_from as date)
        and (customer_history.dbt_valid_to is null
            or sales.sale_date < cast(customer_history.dbt_valid_to as date))
    left join brands
        on sales.brand_code = brands.brand_code

)

select
    cast(sales_line_key as varchar(80)) as sales_line_key,
    cast(source_system as varchar(10)) as source_system,
    cast(document_no as varchar(30)) as document_no,
    cast(document_line_no as int) as document_line_no,
    cast(year(sale_date) * 10000 + month(sale_date) * 100 + day(sale_date) as int) as date_key,
    cast(sale_date as date) as sale_date,
    cast(customer_version_key as varchar(64)) as customer_version_key,
    cast(coalesce(customer_no, 'TAPROOM') as varchar(20)) as customer_no,
    cast(product_code as varchar(40)) as product_key,
    cast(coalesce(brand_code, 'MERCH') as varchar(10)) as brand_code,
    cast(channel as varchar(40)) as channel,
    cast(state_code as varchar(2)) as state_code,
    cast(is_credit as bit) as is_credit,
    cast(quantity as int) as quantity,
    cast(net_amount as decimal(18, 2)) as net_amount,
    cast(barrels as decimal(18, 4)) as barrels,
    cast(case_equivalents as decimal(18, 4)) as case_equivalents,
    cast(cost_amount as decimal(18, 2)) as cost_amount,
    cast(net_amount - cost_amount as decimal(18, 2)) as gross_margin_amount,
    cast(_ingested_at as datetime2(6)) as _ingested_at
from costed
