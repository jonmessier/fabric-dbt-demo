-- Beer sitting in each distributor's warehouse, by brand and month.
--
-- shipped   = what we invoiced to the distributor that month (sell-in)
-- depleted  = what the distributor reported selling to retailers (sell-through)
-- inventory = running total of shipped minus depleted since the start of the data
--             (assumes the distributor started with none of our beer)
-- weeks of supply = ending inventory / average weekly depletions that month
--
-- Months after a distributor's latest report are left out: without depletions they would show
-- every shipment as inventory.

with shipped as (

    select
        distributors.distributor_code,
        sales.brand_code,
        datefromparts(year(sales.sale_date), month(sales.sale_date), 1) as report_month,
        sum(sales.barrels) as shipped_barrels
    from {{ ref('fct_sales_line') }} as sales
    inner join {{ ref('dim_distributor') }} as distributors
        on sales.customer_no = distributors.erp_customer_no
    group by distributors.distributor_code, sales.brand_code,
        datefromparts(year(sales.sale_date), month(sales.sale_date), 1)

),

depleted as (

    select
        distributor_code,
        brand_code,
        report_month,
        sum(barrels) as depleted_barrels
    from {{ ref('fct_depletion') }}
    where is_sku_mapped = 1
    group by distributor_code, brand_code, report_month

),

latest_report as (

    select distributor_code, max(report_month) as latest_report_month
    from {{ ref('fct_depletion') }}
    group by distributor_code

),

combined as (

    select
        coalesce(shipped.distributor_code, depleted.distributor_code) as distributor_code,
        coalesce(shipped.brand_code, depleted.brand_code) as brand_code,
        coalesce(shipped.report_month, depleted.report_month) as report_month,
        coalesce(shipped.shipped_barrels, 0) as shipped_barrels,
        coalesce(depleted.depleted_barrels, 0) as depleted_barrels
    from shipped
    full outer join depleted
        on shipped.distributor_code = depleted.distributor_code
        and shipped.brand_code = depleted.brand_code
        and shipped.report_month = depleted.report_month

),

running as (

    select
        combined.*,
        sum(combined.shipped_barrels - combined.depleted_barrels) over (
            partition by combined.distributor_code, combined.brand_code
            order by combined.report_month
            rows between unbounded preceding and current row
        ) as ending_inventory_barrels
    from combined
    inner join latest_report
        on combined.distributor_code = latest_report.distributor_code
        and combined.report_month <= latest_report.latest_report_month

)

select
    cast(concat(distributor_code, '|', brand_code, '|', concat(year(report_month), '-', right(concat('0', month(report_month)), 2))) as varchar(60))
        as channel_inventory_key,
    cast(year(report_month) * 10000 + month(report_month) * 100 + 1 as int) as month_date_key,
    cast(report_month as date) as report_month,
    cast(distributor_code as varchar(20)) as distributor_code,
    cast(brand_code as varchar(10)) as brand_code,
    cast(shipped_barrels as decimal(18, 4)) as shipped_barrels,
    cast(depleted_barrels as decimal(18, 4)) as depleted_barrels,
    cast(shipped_barrels - depleted_barrels as decimal(18, 4)) as net_change_barrels,
    cast(ending_inventory_barrels as decimal(18, 4)) as ending_inventory_barrels,
    cast(case
        when depleted_barrels > 0
            then ending_inventory_barrels
                / (depleted_barrels * 7.0 / day(eomonth(report_month)))
    end as decimal(9, 2)) as weeks_of_supply
from running
