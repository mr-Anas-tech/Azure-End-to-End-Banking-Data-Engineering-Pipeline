with users as(
    select * from{{ ref("stg_banking_users") }}
),
transactions as(
    select * from {{ref("stg_transactions")}}
),

users_metrix as(
    select
    user_id,
    count(transaction_id) as total_transaction_count,

    sum(case when transaction_type in('Deposit') then amount else 0 end) as total_deposit_amount,
    sum(case when transaction_type in ( 'Withdrawal', 'Transfer', 'Payment') then amount else 0 end) as total_withdrawal_amount,

    sum(amount) as total_volume_amount,
    avg(amount) as avg_volume_amount,
    min(amount) as min_volume_amount,
    max(amount) as max_volume_amount,

    min(transaction_timestamp) as first_transaction_at,
    max(transaction_timestamp) as last_transaction_at

    from transactions
    group by user_id
)

select
u.user_id,
u.name as full_name,
u.national_id,
u.nation_id_type,
u.gender,
u.nationality,
u.email,
u.phone_number,
u.cell_number,
u.user_state,
u.country,
u.age,
u.registration_date,
u.account_age_year,

coalesce(m.total_transaction_count ,0) as total_transaction_count,
coalesce(m.total_deposit_amount, 0) as total_deposit_amount,
coalesce(m.total_withdrawal_amount, 0) as total_withdrawal_amount,
(coalesce(m.total_deposit_amount, 0) - coalesce(m.total_withdrawal_amount, 0)) estimated_net_balance,
coalesce(m.total_volume_amount, 0) as total_volume_processed,
coalesce(m.avg_volume_amount, 0) as avg_volume_amount,
coalesce(m.min_volume_amount, 0) as min_volume_amount,
coalesce(m.max_volume_amount, 0) as max_volume_amount,

m.first_transaction_at,
m.last_transaction_at,
datediff(day, m.last_transaction_at, current_date()) as day_since_last_transaction,

case
when m.total_transaction_count is null or m.total_transaction_count = 0 then 'Inactive / New'
when datediff(day, m.last_transaction_at, current_date()) <= 30 then 'Highly Active'
when datediff(day, m.last_transaction_at, current_date()) <=90 then 'Dormant'
else 'Churned'
end as account_health_status,

case 
when (coalesce(m.total_deposit_amount, 0) - coalesce(m.total_withdrawal_amount, 0))>= 10000 then 'Tier 1 (VIP)'
when (coalesce(m.total_deposit_amount, 0) - coalesce(m.total_withdrawal_amount, 0)) >=3000 then 'Tier 2 (Gold)'
when (coalesce(m.total_deposit_amount, 0) - coalesce(m.total_withdrawal_amount, 0))> 0 then 'Tier 3 (Standard)'
else 'zero_balance' 
end as customer_value_tier,

case 
when( coalesce(m.total_deposit_amount, 0)- coalesce(m.total_withdrawal_amount, 0)) <0 then true 
else false
end as is_overdrawn_flag

from users u
left join users_metrix m 
on u.user_id= m.user_id

