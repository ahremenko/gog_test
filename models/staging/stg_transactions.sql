{{ config(materialized='table') }}

with stg_transactions AS (
    select
        transaction_id,
        user_id,
        game_id,
        transaction_date,
        amount,
        currency,
        payment_method,
        product_type,
        date_trunc(transaction_date, DAY) as transaction_day
    from raw.raw_transactions    
)

select * from stg_transactions