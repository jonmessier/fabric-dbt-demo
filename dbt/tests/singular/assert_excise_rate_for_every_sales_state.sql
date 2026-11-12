-- Every state we sell into needs an excise tax rate in seeds/ref_excise_tax_rate.csv,
-- otherwise excise liability is understated for that state.
--
-- Warn for now so the sales models still build; the excise fact in gold (Phase 5) turns
-- this into an error, because an excise report with a missing state is wrong, not incomplete.
{{ config(severity='warn') }}

select
    sales.state_code,
    count(*) as sales_lines,
    sum(sales.barrels) as barrels
from {{ ref('int_sales_lines_unioned') }} as sales
left join {{ ref('ref_excise_tax_rate') }} as rates
    on sales.state_code = rates.state_code
where sales.state_code is not null
    and rates.state_code is null
group by sales.state_code
