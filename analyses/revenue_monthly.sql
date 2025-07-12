with r as (
    select max(rate_date) as rate_date from {{ ref('exchange_rates')}}
)
select 
  product_type,
  payment_method,
  game_genre,
  date_trunc(transaction_day, MONTH) as transaction_month,
  round(sum(revenue), 2) as revenue
from {{ ref('fact_daily_revenue') }} t
inner join r on t.transaction_day < date_trunc(r.rate_date, MONTH)
group by
  product_type,
  payment_method,
  game_genre,
  date_trunc(transaction_day, MONTH);