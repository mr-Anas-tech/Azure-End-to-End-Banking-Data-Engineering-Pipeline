with transactions as (
    select * from {{ ref('stg_transactions') }}
),

users as (
    select * from {{ ref('stg_banking_users') }}
)

select
    t.transaction_id,
    t.user_id,

    t.transaction_type,
    t.amount,
    t.transaction_timestamp,

    u.name as customer_name,
    u.email as customer_email,
    u.country as customer_country,
    u.city as customer_city,
    u.gender as customer_gender,
    u.age as customer_age

from transactions t
left join users u
    on t.user_id = u.user_id