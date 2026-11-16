-- One row per distributor, month, retail account and distributor product code.
-- Product codes not in the cross-reference seed land on the UNMAPPED product and brand, so their
-- volume stays visible in reports until the seed is fixed.

select
    cast(depletion_key as varchar(160)) as depletion_key,
    cast(year(report_month) * 10000 + month(report_month) * 100 + 1 as int) as month_date_key,
    cast(report_month as date) as report_month,
    cast(distributor_code as varchar(20)) as distributor_code,
    cast(concat(distributor_code, '|', distributor_account_id) as varchar(64)) as retail_account_key,
    cast(coalesce(item_no, 'UNMAPPED') as varchar(40)) as product_key,
    cast(coalesce(brand_code, 'UNMAPPED') as varchar(10)) as brand_code,
    cast(distributor_sku as varchar(40)) as distributor_sku,
    cast(is_sku_mapped as bit) as is_sku_mapped,
    cast(quantity as int) as quantity,
    cast(unit_type as varchar(10)) as unit_type,
    cast(barrels as decimal(18, 4)) as barrels,
    cast(case_equivalents as decimal(18, 4)) as case_equivalents
from {{ ref('int_depletions_mapped') }}
