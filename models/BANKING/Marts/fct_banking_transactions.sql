with transactions as (
    select * from {{ ref('stg_transactions') }}
),

users as (
    select * from {{ ref('stg_banking_users') }}
),

joined_as(

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
    u.age as customer_age,
    Row_NUMBER() OVER(
        partition by t.transaction_id
        ORDER by t.transaction_timestamp DESC
    ) AS rn

from transactions t
left join users u
    on t.user_id = u.user_id
)


select
transaction_id,
user_id,
transaction_type,
amount,
transaction_timestamp,
customer_name,
customer_email,
customer_country,
customer_city,
customer_gender,
customer_age
from joined_as
where rn = 1


