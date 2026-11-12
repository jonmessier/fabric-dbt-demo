{#
    The ERP overwrites customer records, so this snapshot is the only place their history lives.
    With the timestamp strategy, dbt_valid_from is the ERP's updated_at, so the first version of
    each customer is valid from when it was created and older orders still find the right row.
#}
{% snapshot snap_erp_customer %}

{{
    config(
        unique_key='customer_no',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    customer_no,
    customer_name,
    customer_type,
    address_line1,
    city,
    state_text,
    postal_code,
    price_tier,
    territory,
    salesperson,
    payment_terms,
    updated_at
from {{ ref('stg_erp__customer') }}

{% endsnapshot %}
