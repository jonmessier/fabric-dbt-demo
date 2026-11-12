-- Sales channels (Wholesale, Taproom, Direct - On premise, ...) and the group each belongs to.

select distinct
    cast(channel as varchar(40)) as channel,
    cast(channel_group as varchar(40)) as channel_group
from {{ ref('ref_channel') }}
