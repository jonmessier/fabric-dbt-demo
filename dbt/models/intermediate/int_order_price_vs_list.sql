-- Price realization: what each invoice line was billed against the list price that applied
-- when the order was placed, for the customer's price tier at that moment.
--
-- Both lookups are point-in-time on the order date:
--   * price tier from the customer snapshot (the ERP overwrites tier changes)
--   * list price from the effective-dated ERP price list
-- Credit memos are left out; returns do not say anything about pricing.

with invoice_lines as (

    select * from {{ ref('stg_erp__invoice_line') }}
    where is_credit = 0

),

orders as (

    select * from {{ ref('stg_erp__sales_header') }}

),

customer_history as (

    select * from {{ ref('snap_erp_customer') }}

),

price_list as (

    select * from {{ ref('stg_erp__price_list') }}

),

priced as (

    select
        invoice_lines.invoice_no,
        invoice_lines.invoice_line_no,
        invoice_lines.invoice_date,
        invoice_lines.order_no,
        orders.order_date,
        invoice_lines.customer_no,
        customer_history.price_tier,
        invoice_lines.item_no,
        invoice_lines.quantity,
        invoice_lines.unit_price as invoiced_unit_price,
        invoice_lines.line_discount_pct,
        invoice_lines.line_amount as net_amount,
        price_list.list_price
    from invoice_lines
    left join orders
        on invoice_lines.order_no = orders.order_no
    left join customer_history
        on invoice_lines.customer_no = customer_history.customer_no
        and orders.order_date >= cast(customer_history.dbt_valid_from as date)
        and (customer_history.dbt_valid_to is null
            or orders.order_date < cast(customer_history.dbt_valid_to as date))
    left join price_list
        on invoice_lines.item_no = price_list.item_no
        and customer_history.price_tier = price_list.price_tier
        and orders.order_date >= price_list.starting_date
        and (price_list.ending_date is null or orders.order_date <= price_list.ending_date)

)

select
    cast(concat(invoice_no, '|', cast(invoice_line_no as varchar(12))) as varchar(40)) as invoice_line_key,
    *,
    cast(quantity * list_price as decimal(18, 2)) as list_amount,
    cast(net_amount - quantity * list_price as decimal(18, 2)) as price_variance_amount,
    cast(case
        when list_price > 0 and quantity <> 0
            then net_amount / (quantity * list_price)
    end as decimal(9, 4)) as price_realization_pct
from priced
