with r as (
    select max(rate_date) as rate_date from {{ ref('exchange_rates')}}
)
  select 
    product_type,
    game_genre,
    round(sum(revenue),2) as revenue
  from {{ ref('fact_daily_revenue') }}, r
  where transaction_day <= r.rate_date
  group by product_type, game_genre
  order by 3 desc