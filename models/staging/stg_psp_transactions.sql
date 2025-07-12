{{ config(materialized='table') }}

with psp_transactions as (
    select
        psp_transaction_id,
        original_transaction_id,
        psp_amount,
        psp_currency,
        psp_timestamp,
        status
    from raw.psp_transactions
)

select * from psp_transactions