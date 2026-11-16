-- One row per ERP invoice line (returns excluded): billed amount against list price at the order
-- date for the customer's price tier on that date.

select
    cast(invoice_line_key as varchar(40)) as invoice_line_key,
    cast(year(order_date) * 10000 + month(order_date) * 100 + day(order_date) as int) as order_date_key,
    cast(order_date as date) as order_date,
    cast(invoice_date as date) as invoice_date,
    cast(order_no as varchar(20)) as order_no,
    cast(customer_version_key as varchar(64)) as customer_version_key,
    cast(customer_no as varchar(20)) as customer_no,
    cast(item_no as varchar(40)) as product_key,
    cast(price_tier as varchar(5)) as price_tier,
    cast(quantity as int) as quantity,
    cast(list_price as decimal(18, 2)) as list_price,
    cast(invoiced_unit_price as decimal(18, 2)) as invoiced_unit_price,
    cast(line_discount_pct as decimal(5, 2)) as line_discount_pct,
    cast(net_amount as decimal(18, 2)) as net_amount,
    cast(list_amount as decimal(18, 2)) as list_amount,
    cast(price_variance_amount as decimal(18, 2)) as price_variance_amount,
    cast(price_realization_pct as decimal(9, 4)) as price_realization_pct
from {{ ref('int_order_price_vs_list') }}
