-- Gold ERP revenue must equal what the ERP posted, to the cent. Any difference means a line was
-- dropped, duplicated or changed between staging and gold. Returns a row only when they differ.

with erp as (

    select sum(line_amount) as erp_net_amount
    from {{ ref('stg_erp__invoice_line') }}

),

gold as (

    select sum(net_amount) as gold_net_amount
    from {{ ref('fct_sales_line') }}
    where source_system = 'ERP'

)

select
    erp.erp_net_amount,
    gold.gold_net_amount,
    gold.gold_net_amount - erp.erp_net_amount as difference
from erp
cross join gold
where coalesce(gold.gold_net_amount, 0) <> coalesce(erp.erp_net_amount, 0)
