-- Check 1: Net Balance Integrity Check
select
    'Net Balance Mismatch' as failure_reason,
    user_id as entity_id,
    cast(estimated_net_balance as string) as details
from {{ ref('dim_customer_360') }}
where estimated_net_balance != (total_deposit_amount - total_withdrawal_amount)

union all

-- Check 2: Negative Transaction Amount Check
select
    'Negative Transaction Amount' as failure_reason,
    transaction_id as entity_id,
    cast(amount as string) as details
from {{ ref('fct_banking_transactions') }}
where amount < 0

union all

-- Check 3: Orphan Transactions Check (Unmatched Users)
select
    'Orphan Transaction (User Not Found)' as failure_reason,
    t.transaction_id as entity_id,
    cast(t.user_id as string) as details
from {{ ref('fct_banking_transactions') }} t
left join {{ ref('dim_customer_360') }} u
    on t.user_id = u.user_id
where u.user_id is null