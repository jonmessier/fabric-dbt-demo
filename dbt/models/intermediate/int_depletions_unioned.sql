-- The three distributor reports, already in one shape from staging, stacked together.

with northland as (

    select * from {{ ref('stg_dist__depletion_northland') }}

),

badger as (

    select * from {{ ref('stg_dist__depletion_badger') }}

),

hawkeye as (

    select * from {{ ref('stg_dist__depletion_hawkeye') }}

),

unioned as (

    {% for relation in ['northland', 'badger', 'hawkeye'] %}
    select
        distributor_code,
        report_month,
        distributor_account_id,
        account_name,
        city,
        state_code,
        premise_type,
        distributor_sku,
        product_description,
        quantity,
        unit_type,
        _source_file,
        _ingested_at
    from {{ relation }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

)

select * from unioned
