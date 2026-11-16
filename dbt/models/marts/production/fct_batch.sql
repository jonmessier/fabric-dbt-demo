-- One row per brew batch with volumes, yield, loss and cost per packaged barrel.
-- Built from int_batch_summary, so when its yield test fails (more packaged than brewed),
-- dbt build skips this model and production reporting stops until the data is fixed.

select
    cast(batch_id as varchar(20)) as batch_id,
    cast(brand_code as varchar(10)) as brand_code,
    cast(year(brew_date) * 10000 + month(brew_date) * 100 + day(brew_date) as int) as brew_date_key,
    cast(case when package_date is not null
        then year(package_date) * 10000 + month(package_date) * 100 + day(package_date)
    end as int) as package_date_key,
    cast(brew_date as date) as brew_date,
    cast(package_date as date) as package_date,
    cast(batch_status as varchar(20)) as batch_status,
    cast(recipe_version as varchar(10)) as recipe_version,
    cast(tank_id as varchar(10)) as tank_id,
    cast(planned_volume_bbl as decimal(9, 2)) as planned_volume_bbl,
    cast(actual_volume_bbl as decimal(9, 2)) as brewed_volume_bbl,
    cast(packaged_volume_bbl as decimal(9, 2)) as packaged_volume_bbl,
    cast(loss_bbl as decimal(9, 2)) as loss_bbl,
    cast(yield_pct as decimal(9, 4)) as yield_pct,
    cast(units_packaged as int) as units_packaged,
    cast(ingredient_cost as decimal(18, 2)) as ingredient_cost,
    cast(labor_cost as decimal(18, 2)) as labor_cost,
    cast(overhead_cost as decimal(18, 2)) as overhead_cost,
    cast(packaging_material_cost as decimal(18, 2)) as packaging_material_cost,
    cast(total_cost as decimal(18, 2)) as total_cost,
    cast(cost_per_packaged_bbl as decimal(18, 2)) as cost_per_packaged_bbl
from {{ ref('int_batch_summary') }}
