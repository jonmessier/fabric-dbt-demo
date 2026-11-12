-- Excise tax by state and month: net barrels sold (returns reduce it) times the state rate.
-- Rates are illustrative demo values from ref_excise_tax_rate, not actual tax rates.
-- A state with sales but no rate gets a null rate and amount, and the not_null test on
-- rate_per_barrel fails the build: an excise report with a missing state is wrong, not incomplete.

with monthly as (

    select
        state_code,
        datefromparts(year(sale_date), month(sale_date), 1) as sales_month,
        sum(barrels) as net_barrels
    from {{ ref('fct_sales_line') }}
    where barrels is not null
        and state_code is not null
    group by state_code, datefromparts(year(sale_date), month(sale_date), 1)

),

rates as (

    select * from {{ ref('ref_excise_tax_rate') }}

)

select
    cast(concat(monthly.state_code, '|', concat(year(monthly.sales_month), '-', right(concat('0', month(monthly.sales_month)), 2))) as varchar(20))
        as excise_key,
    cast(year(monthly.sales_month) * 10000 + month(monthly.sales_month) * 100 + 1 as int) as month_date_key,
    cast(monthly.sales_month as date) as sales_month,
    cast(monthly.state_code as varchar(2)) as state_code,
    cast(monthly.net_barrels as decimal(18, 4)) as net_barrels,
    cast(rates.rate_per_barrel as decimal(9, 4)) as rate_per_barrel,
    cast(monthly.net_barrels * rates.rate_per_barrel as decimal(18, 2)) as excise_tax_amount
from monthly
left join rates
    on monthly.state_code = rates.state_code
    and monthly.sales_month >= rates.effective_from
