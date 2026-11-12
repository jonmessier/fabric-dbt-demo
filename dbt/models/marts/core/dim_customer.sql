-- ERP customers as a type 2 dimension: one row per customer per version, from the snapshot.
-- Facts join to the version that was valid on the sale (or order) date, so a customer who moved
-- territory or changed price tier keeps their old attributes on their old sales.
-- TAPROOM is a single member for walk-in taproom sales.

with history as (

    select * from {{ ref('snap_erp_customer') }}

),

states as (

    select * from {{ ref('ref_state_region') }}

),

channels as (

    select * from {{ ref('ref_channel') }}
    where source_system = 'ERP'

),

versions as (

    select
        history.dbt_scd_id as customer_version_key,
        history.customer_no,
        history.customer_name,
        history.customer_type,
        channels.channel,
        history.city,
        states.state_code,
        states.sales_region,
        history.price_tier,
        history.territory,
        history.salesperson,
        history.dbt_valid_from as valid_from,
        history.dbt_valid_to as valid_to,
        case when history.dbt_valid_to is null then 1 else 0 end as is_current
    from history
    left join states
        on history.state_text = states.state_text
    left join channels
        on history.customer_type = channels.customer_type

),

members as (

    select * from versions

    union all

    select
        'TAPROOM', 'TAPROOM', 'Taproom walk-in', 'TAPROOM', 'Taproom', null, 'MN', 'Minnesota',
        null, 'TAPROOM', null, cast('1900-01-01' as datetime2(6)), null, 1

)

select
    cast(customer_version_key as varchar(64)) as customer_version_key,
    cast(customer_no as varchar(20)) as customer_no,
    cast(customer_name as varchar(120)) as customer_name,
    cast(customer_type as varchar(20)) as customer_type,
    cast(channel as varchar(40)) as channel,
    cast(city as varchar(60)) as city,
    cast(state_code as varchar(2)) as state_code,
    cast(sales_region as varchar(40)) as sales_region,
    cast(price_tier as varchar(5)) as price_tier,
    cast(territory as varchar(30)) as territory,
    cast(salesperson as varchar(60)) as salesperson,
    cast(valid_from as datetime2(6)) as valid_from,
    cast(valid_to as datetime2(6)) as valid_to,
    cast(is_current as bit) as is_current
from members
