{{ config(materialized='table') }}

with exchange_rates as (
    select 
        date as rate_date,
        currency_from,
        currency_to,
        rate
    from raw.exchange_rates    
)

select * from exchange_rates