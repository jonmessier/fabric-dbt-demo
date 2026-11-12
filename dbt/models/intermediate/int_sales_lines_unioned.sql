-- Every sale from both selling systems at one grain: one row per invoice line (ERP) or
-- per taproom line item (POS), with volume converted to barrels and case equivalents
-- through the same seed, and the sales channel attached.
--
-- Returns are ERP credit memo lines: negative quantity, amount and barrels.
-- Refunded taproom transactions are left out. Merchandise stays in, with null volume.

with invoice_lines as (

    select * from {{ ref('stg_erp__invoice_line') }}

),

items as (

    select * from {{ ref('stg_erp__item') }}

),

customers as (

    select * from {{ ref('stg_erp__customer') }}

),

states as (

    select * from {{ ref('ref_state_region') }}

),

pos_lines as (

    select * from {{ ref('stg_pos__line_item') }}

),

pos_transactions as (

    select * from {{ ref('stg_pos__transaction') }}

),

uom as (

    select * from {{ ref('ref_uom_conversion') }}

),

channels as (

    select * from {{ ref('ref_channel') }}

),

erp_sales as (

    select
        cast('ERP' as varchar(10)) as source_system,
        invoice_lines.invoice_no as document_no,
        invoice_lines.invoice_line_no as document_line_no,
        invoice_lines.invoice_date as sale_date,
        invoice_lines.order_no,
        invoice_lines.customer_no,
        customers.customer_type,
        states.state_code,
        invoice_lines.item_no,
        items.brand_code,
        items.package_code,
        invoice_lines.is_credit,
        invoice_lines.quantity,
        invoice_lines.line_amount as net_amount,
        cast(invoice_lines.quantity * uom.barrels as decimal(18, 4)) as barrels,
        cast(invoice_lines.quantity * uom.case_equivalents as decimal(18, 4)) as case_equivalents,
        cast(invoice_lines.quantity * items.standard_cost as decimal(18, 2)) as standard_cost_amount
    from invoice_lines
    left join items
        on invoice_lines.item_no = items.item_no
    left join uom
        on items.package_code = uom.package_code
    left join customers
        on invoice_lines.customer_no = customers.customer_no
    left join states
        on customers.state_text = states.state_text

),

pos_sales as (

    select
        cast('POS' as varchar(10)) as source_system,
        pos_lines.transaction_id as document_no,
        pos_lines.line_id as document_line_no,
        pos_transactions.transaction_date as sale_date,
        cast(null as varchar(20)) as order_no,
        cast(null as varchar(20)) as customer_no,
        cast('TAPROOM' as varchar(20)) as customer_type,
        -- the taproom is at the brewery
        cast('MN' as varchar(2)) as state_code,
        cast(null as varchar(20)) as item_no,
        pos_lines.brand_code,
        pos_lines.package_code,
        cast(0 as bit) as is_credit,
        pos_lines.quantity,
        pos_lines.line_amount as net_amount,
        cast(pos_lines.quantity * uom.barrels as decimal(18, 4)) as barrels,
        cast(pos_lines.quantity * uom.case_equivalents as decimal(18, 4)) as case_equivalents,
        -- taproom cost comes from production cost per barrel in gold, not from an item standard cost
        cast(null as decimal(18, 2)) as standard_cost_amount
    from pos_lines
    inner join pos_transactions
        on pos_lines.transaction_id = pos_transactions.transaction_id
    left join uom
        on pos_lines.package_code = uom.package_code
    where pos_transactions.transaction_status = 'COMPLETED'

),

unioned as (

    select * from erp_sales
    union all
    select * from pos_sales

)

select
    cast(concat(unioned.source_system, '|', unioned.document_no, '|', cast(unioned.document_line_no as varchar(12)))
        as varchar(80)) as sales_line_key,
    unioned.*,
    channels.channel,
    channels.channel_group
from unioned
left join channels
    on unioned.source_system = channels.source_system
    and unioned.customer_type = channels.customer_type
