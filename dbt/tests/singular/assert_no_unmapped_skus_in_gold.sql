-- Distributor product codes that reached gold without a match in ref_distributor_sku_xref.
-- Their volume sits on the UNMAPPED product, so brand and channel-inventory numbers are understated.
-- Fix: add the codes to seeds/ref_distributor_sku_xref.csv, then dbt build -s ref_distributor_sku_xref+
{{ config(severity='warn') }}

select
    distributor_code,
    distributor_sku,
    count(*) as depletion_rows,
    sum(quantity) as units
from {{ ref('fct_depletion') }}
where is_sku_mapped = 0
group by distributor_code, distributor_sku
