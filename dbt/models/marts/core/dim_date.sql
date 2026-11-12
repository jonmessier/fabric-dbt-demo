-- One row per calendar day across the fiscal calendar seed, with calendar and 4-4-5 fiscal attributes.
-- The date range comes from ref_fiscal_calendar_445, so extending the calendar is a seed change.

with bounds as (

    select
        min(start_date) as first_date,
        max(end_date) as last_date
    from {{ ref('ref_fiscal_calendar_445') }}

),

digits as (

    select 0 as n union all select 1 union all select 2 union all select 3 union all select 4
    union all select 5 union all select 6 union all select 7 union all select 8 union all select 9

),

numbers as (

    select ones.n + 10 * tens.n + 100 * hundreds.n + 1000 * thousands.n as n
    from digits as ones
    cross join digits as tens
    cross join digits as hundreds
    cross join digits as thousands

),

dates as (

    select dateadd(day, numbers.n, bounds.first_date) as calendar_date
    from numbers
    cross join bounds
    where numbers.n <= datediff(day, bounds.first_date, bounds.last_date)

),

fiscal as (

    select * from {{ ref('ref_fiscal_calendar_445') }}

),

enriched as (

    select
        dates.calendar_date,
        year(dates.calendar_date) as calendar_year,
        month(dates.calendar_date) as calendar_month,
        day(dates.calendar_date) as day_of_month,
        -- 1900-01-01 was a Monday, so this is 1 = Monday ... 7 = Sunday whatever DATEFIRST is set to
        datediff(day, cast('1900-01-01' as date), dates.calendar_date) % 7 + 1 as day_of_week,
        fiscal.fiscal_year,
        fiscal.fiscal_quarter,
        fiscal.fiscal_period,
        fiscal.period_name,
        fiscal.start_date as fiscal_period_start_date,
        fiscal.end_date as fiscal_period_end_date
    from dates
    left join fiscal
        on dates.calendar_date between fiscal.start_date and fiscal.end_date

)

select
    cast(calendar_year * 10000 + calendar_month * 100 + day_of_month as int) as date_key,
    cast(calendar_date as date) as calendar_date,
    cast(calendar_year as int) as calendar_year,
    cast(datepart(quarter, calendar_date) as int) as calendar_quarter,
    cast(calendar_month as int) as calendar_month,
    cast(case calendar_month
        when 1 then 'January' when 2 then 'February' when 3 then 'March' when 4 then 'April'
        when 5 then 'May' when 6 then 'June' when 7 then 'July' when 8 then 'August'
        when 9 then 'September' when 10 then 'October' when 11 then 'November' when 12 then 'December'
    end as varchar(10)) as month_name,
    cast(datefromparts(calendar_year, calendar_month, 1) as date) as month_start_date,
    cast(day_of_week as int) as day_of_week,
    cast(case day_of_week
        when 1 then 'Monday' when 2 then 'Tuesday' when 3 then 'Wednesday' when 4 then 'Thursday'
        when 5 then 'Friday' when 6 then 'Saturday' when 7 then 'Sunday'
    end as varchar(10)) as day_name,
    cast(case when day_of_week >= 6 then 1 else 0 end as bit) as is_weekend,
    cast(fiscal_year as int) as fiscal_year,
    cast(fiscal_quarter as int) as fiscal_quarter,
    cast(fiscal_period as int) as fiscal_period,
    cast(period_name as varchar(12)) as fiscal_period_name,
    cast(fiscal_period_start_date as date) as fiscal_period_start_date,
    cast(fiscal_period_end_date as date) as fiscal_period_end_date
from enriched
