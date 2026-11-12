-- One row per product as it is sold: ERP items (packages), taproom serving sizes and merchandise
-- as seen in sales, plus an UNMAPPED member for distributor product codes that are not in the
-- cross-reference seed yet.

with items as (

    select * from {{ ref('stg_erp__item') }}

),

uom as (

    select * from {{ ref('ref_uom_conversion') }}

),

taproom as (

    select
        product_code,
        max(product_name) as product_name,
        max(brand_code) as brand_code,
        max(package_code) as package_code
    from {{ ref('int_sales_lines_unioned') }}
    where source_system = 'POS'
    group by product_code

),

members as (

    select
        items.item_no as product_key,
        items.item_no,
        items.item_description as product_name,
        items.brand_code,
        items.package_code,
        'ERP' as product_source,
        items.standard_cost
    from items

    union all

    select
        taproom.product_code,
        null,
        taproom.product_name,
        coalesce(taproom.brand_code, 'MERCH'),
        taproom.package_code,
        'TAPROOM',
        null
    from taproom

    union all

    select 'UNMAPPED', null, 'Unmapped distributor product', 'UNMAPPED', null, 'UNMAPPED', null

)

select
    cast(members.product_key as varchar(40)) as product_key,
    cast(members.item_no as varchar(20)) as item_no,
    cast(members.product_name as varchar(160)) as product_name,
    cast(members.brand_code as varchar(10)) as brand_code,
    cast(members.package_code as varchar(20)) as package_code,
    cast(uom.description as varchar(60)) as package_description,
    cast(coalesce(uom.unit_type, 'MERCH') as varchar(10)) as unit_type,
    cast(uom.barrels as decimal(12, 6)) as barrels_per_unit,
    cast(members.product_source as varchar(10)) as product_source,
    cast(members.standard_cost as decimal(18, 2)) as standard_cost
from members
left join uom
    on members.package_code = uom.package_code
