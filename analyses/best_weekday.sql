with r as (
    select max(rate_date) as rate_date from {{ ref('exchange_rates')}}
)
, t as (
  select 
    transaction_day,
    sum(revenue) as revenue
  from {{ ref('fact_daily_revenue') }}, r
  where transaction_day <= r.rate_date
  group by transaction_day
) 
select 
   format_date('%A', transaction_day) as week_day,
   round(sum(revenue), 2) as revenue
 from t
 group by format_date('%A', transaction_day)
 order by 2 desc