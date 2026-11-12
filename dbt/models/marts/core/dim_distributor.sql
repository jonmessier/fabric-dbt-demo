-- The three wholesale distributors.

select
    cast(distributor_code as varchar(20)) as distributor_code,
    cast(distributor_name as varchar(100)) as distributor_name,
    cast(erp_customer_no as varchar(20)) as erp_customer_no,
    cast(state_code as varchar(2)) as state_code
from {{ ref('ref_distributor') }}
