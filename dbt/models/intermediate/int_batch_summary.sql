-- One row per brew batch: what went into the fermenter, what came out in packages, and what it cost.
-- Yield = packaged barrels / brewed barrels. It can only be above 1 when a packaging run was keyed
-- wrong, which is why the between(0, 1) test on it is an error: the cost per barrel of every
-- downstream production number would be wrong too.

with batches as (

    select * from {{ ref('stg_prod__batch') }}

),

packaging as (

    select
        batch_id,
        count(*) as packaging_runs,
        sum(units_packaged) as units_packaged,
        sum(packaged_volume_bbl) as packaged_volume_bbl
    from {{ ref('stg_prod__packaging_run') }}
    group by batch_id

),

costs as (

    select
        batch_id,
        sum(case when cost_type = 'INGREDIENTS' then cost_amount else 0 end) as ingredient_cost,
        sum(case when cost_type = 'LABOR' then cost_amount else 0 end) as labor_cost,
        sum(case when cost_type = 'OVERHEAD' then cost_amount else 0 end) as overhead_cost,
        sum(case when cost_type = 'PACKAGING_MATERIALS' then cost_amount else 0 end) as packaging_material_cost,
        sum(cost_amount) as total_cost
    from {{ ref('stg_prod__batch_cost') }}
    group by batch_id

)

select
    batches.batch_id,
    batches.brand_code,
    batches.recipe_version,
    batches.batch_status,
    batches.brew_date,
    batches.package_date,
    batches.tank_id,
    batches.planned_volume_bbl,
    batches.actual_volume_bbl,
    coalesce(packaging.packaging_runs, 0) as packaging_runs,
    coalesce(packaging.units_packaged, 0) as units_packaged,
    coalesce(packaging.packaged_volume_bbl, 0) as packaged_volume_bbl,
    cast(case
        when batches.batch_status = 'PACKAGED' and batches.actual_volume_bbl > 0
            then packaging.packaged_volume_bbl / batches.actual_volume_bbl
    end as decimal(9, 4)) as yield_pct,
    cast(case
        when batches.batch_status = 'PACKAGED'
            then batches.actual_volume_bbl - packaging.packaged_volume_bbl
    end as decimal(9, 2)) as loss_bbl,
    costs.ingredient_cost,
    costs.labor_cost,
    costs.overhead_cost,
    costs.packaging_material_cost,
    costs.total_cost,
    cast(case
        when packaging.packaged_volume_bbl > 0
            then costs.total_cost / packaging.packaged_volume_bbl
    end as decimal(18, 2)) as cost_per_packaged_bbl
from batches
left join packaging
    on batches.batch_id = packaging.batch_id
left join costs
    on batches.batch_id = costs.batch_id
