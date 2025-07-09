with t as (
  select 
    format_date('%B', transaction_day) as month,
    sum(revenue) as revenue
  from {{ ref('fact_daily_revenue') }}
  where transaction_day < trunc_date(current_date(), MONTH)
  group by format_date('%B', transaction_day)
) 
select 
   month,
   round(revenue, 2) as revenue
from t
order by 2 desc;