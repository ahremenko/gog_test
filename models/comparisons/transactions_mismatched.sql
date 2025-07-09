with raw_only as (
    select
        transaction_id
    from {{ ref('stg_transactions') }}
    except distinct
    select
        original_transaction_id
    from {{ ref('stg_psp_transactions') }}
)
, psp_only as (
    select
        original_transaction_id
    from {{ ref('stg_psp_transactions') }}
    except distinct    
    select
        transaction_id
    from {{ ref('stg_transactions') }}
)
select 
    transaction_id,
    'raw_transaction ONLY' as source
from raw_only
where transaction_id is not null
union all
select 
    original_transaction_id,
    'psp_transaction ONLY' as source
from psp_only
where original_transaction_id is not null
