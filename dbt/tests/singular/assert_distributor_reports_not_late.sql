-- Every distributor should have reported the same latest month.
-- Returns one row per distributor that is behind the others, so a late file shows up as a
-- warning in the run (and in audit.assert_distributor_reports_not_late) instead of as
-- silently missing sales in the channel reports.
--
-- Source freshness cannot catch this one: it measures when rows were loaded,
-- and a late monthly file is about which business month is missing.
{{ config(severity='warn') }}

with reports as (

    select distributor_code, report_month from {{ ref('stg_dist__depletion_northland') }}
    union all
    select distributor_code, report_month from {{ ref('stg_dist__depletion_badger') }}
    union all
    select distributor_code, report_month from {{ ref('stg_dist__depletion_hawkeye') }}

),

latest as (

    select
        distributor_code,
        max(report_month) as latest_report_month
    from reports
    group by distributor_code

),

expected as (

    select max(latest_report_month) as expected_report_month
    from latest

)

select
    latest.distributor_code,
    latest.latest_report_month,
    expected.expected_report_month,
    datediff(month, latest.latest_report_month, expected.expected_report_month) as months_behind
from latest
cross join expected
where latest.latest_report_month < expected.expected_report_month
