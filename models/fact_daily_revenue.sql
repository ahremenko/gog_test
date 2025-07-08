with rates_to as (
  select distinct currency_to from {{ ref('exchange_rates') }}
)
, rates as (
  select rate_date, currency_from, currency_to, rate from {{ ref('exchange_rates') }}
  where rate_date not in (date '2025-06-26',  date '2025-07-01', date '2025-07-02')
)
, pre_transactions as (
  select
    t.game_id, 
    t.transaction_day, 
    t.amount, 
    t.currency, 
    t.payment_method, 
    t.product_type,
    rt.currency_to
  from rates_to rt, {{ ref('stg_transactions') }} t 
)
, pre_transactions_rates as (
    select
        t.game_id, 
        t.transaction_day, 
        t.amount, 
        t.currency, 
        t.payment_method, 
        t.product_type,
        t.currency_to,
        r.rate
    from pre_transactions t
    left join rates r on r.currency_to = t.currency_to and r.currency_from = t.currency and r.rate_date = t.transaction_day   
)
, transactions as (
    select
        t.game_id, 
        t.transaction_day, 
        t.amount, 
        t.currency, 
        t.payment_method, 
        t.product_type,
        t.currency_to,
        t.rate,
        last_value(t.rate ignore nulls) over (partition by t.currency, t.currency_to order by t.transaction_day RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_rate
    from pre_transactions_rates t
)
, dim_games as (
  select 
    game_id, 
    genre
  from {{ ref('dim_games') }}  
)
select 
   t.product_type,
   t.payment_method,
   g.genre,
   t.transaction_day,
   t.currency_to as currency,
   sum(coalesce(t.rate, t.last_rate) * t.amount) as revenue
from transactions t
join dim_games g on g.game_id = t.game_id
group by    
   t.product_type,
   t.payment_method,
   g.genre,
   t.transaction_day,
   t.currency_to




