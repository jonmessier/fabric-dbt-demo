-- Bars, restaurants and stores our distributors sell to, as each distributor identifies them,
-- with the cleaned name and premise type from the account cross-reference seed.

select
    cast(concat(distributor_code, '|', distributor_account_id) as varchar(64)) as retail_account_key,
    cast(distributor_code as varchar(20)) as distributor_code,
    cast(distributor_account_id as varchar(40)) as distributor_account_id,
    cast(max(account_name) as varchar(120)) as account_name,
    cast(max(premise_type) as varchar(20)) as premise_type,
    cast(max(city) as varchar(60)) as city,
    cast(max(state_code) as varchar(2)) as state_code
from {{ ref('int_depletions_mapped') }}
group by distributor_code, distributor_account_id
