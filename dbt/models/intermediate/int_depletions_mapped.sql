-- Depletions translated into our own product and account terms through the business-owned
-- cross-reference seeds. A distributor code missing from ref_distributor_sku_xref keeps its row
-- (so volume is never silently dropped) with is_sku_mapped = 0 and no item or barrels.

with depletions as (

    select * from {{ ref('int_depletions_unioned') }}

),

sku_xref as (

    select * from {{ ref('ref_distributor_sku_xref') }}

),

account_xref as (

    select * from {{ ref('ref_distributor_account_xref') }}

),

items as (

    select * from {{ ref('stg_erp__item') }}

),

uom as (

    select * from {{ ref('ref_uom_conversion') }}

)

select
    cast(concat(depletions.distributor_code, '|', concat(year(depletions.report_month), '-', right(concat('0', month(depletions.report_month)), 2)), '|',
        depletions.distributor_account_id, '|', depletions.distributor_sku) as varchar(160)) as depletion_key,
    depletions.distributor_code,
    depletions.report_month,
    depletions.distributor_account_id,
    coalesce(account_xref.account_name, depletions.account_name) as account_name,
    coalesce(account_xref.premise_type, depletions.premise_type) as premise_type,
    coalesce(account_xref.city, depletions.city) as city,
    coalesce(account_xref.state_code, depletions.state_code) as state_code,
    depletions.distributor_sku,
    depletions.product_description,
    sku_xref.item_no,
    items.brand_code,
    items.package_code,
    cast(case when sku_xref.item_no is null then 0 else 1 end as bit) as is_sku_mapped,
    depletions.quantity,
    depletions.unit_type,
    cast(depletions.quantity * uom.barrels as decimal(18, 4)) as barrels,
    cast(depletions.quantity * uom.case_equivalents as decimal(18, 4)) as case_equivalents,
    depletions._source_file,
    depletions._ingested_at
from depletions
left join sku_xref
    on depletions.distributor_code = sku_xref.distributor_code
    and depletions.distributor_sku = sku_xref.distributor_sku
left join account_xref
    on depletions.distributor_code = account_xref.distributor_code
    and depletions.distributor_account_id = account_xref.distributor_account_id
left join items
    on sku_xref.item_no = items.item_no
left join uom
    on items.package_code = uom.package_code
