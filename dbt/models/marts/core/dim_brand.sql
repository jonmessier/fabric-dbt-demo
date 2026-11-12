-- One row per brand (the liquid), with the standard cost of one barrel of it.
-- Cost per barrel comes from ERP standard costs, averaged over the brand's packages, so taproom
-- pours can be costed the same way as packaged sales. Two members cover rows with no brand:
-- MERCH (taproom merchandise) and UNMAPPED (distributor codes not yet in the cross-reference).

with items as (

    select * from {{ ref('stg_erp__item') }}

),

uom as (

    select * from {{ ref('ref_uom_conversion') }}

),

brands as (

    select
        items.brand_code,
        max(items.brand_name) as brand_name,
        max(items.style) as style,
        max(items.product_category) as product_category,
        max(items.abv) as abv,
        avg(items.standard_cost / uom.barrels) as standard_cost_per_bbl
    from items
    inner join uom
        on items.package_code = uom.package_code
    group by items.brand_code

),

members as (

    select brand_code, brand_name, style, product_category, abv, standard_cost_per_bbl
    from brands

    union all

    select 'MERCH', 'Merchandise', 'Merchandise', 'MERCH', null, null

    union all

    select 'UNMAPPED', 'Unmapped distributor product', 'Unknown', 'UNKNOWN', null, null

)

select
    cast(brand_code as varchar(10)) as brand_code,
    cast(brand_name as varchar(80)) as brand_name,
    cast(style as varchar(40)) as style,
    cast(product_category as varchar(20)) as product_category,
    cast(abv as decimal(4, 1)) as abv,
    cast(standard_cost_per_bbl as decimal(18, 2)) as standard_cost_per_bbl
from members
