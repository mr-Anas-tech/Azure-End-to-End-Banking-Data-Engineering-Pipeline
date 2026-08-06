with staging_users as (
    select * from {{ ref('stg_banking_users') }}
),

intermediate_summary as (
    select * from {{ ref('int_users_transactions_summary') }}
)

select
    u.user_id,

    u.national_id,
    u.nation_id_type,
    u.title,
    u.name as full_name,
    u.gender,
    u.nationality,
    u.email,
    u.cell_number,
    u.phone_number,

    u.street_name,
    u.street_number,
    u.city,
    u.user_state,
    u.country,
    u.postal_code,
    u.coordinate_latitude,
    u.coordinate_longitude,
    u.timezone_description,
    u.timezone_offset,

    u.date_of_birth,
    u.age,
    u.registration_date,
    u.account_age_year,

    u.username,
    u.password,
    u.password_salt,
    u.md5_hash,
    u.profile_pic_large,
    u.profile_pic_medium,
    u.profile_pic_thumbnail,

    coalesce(i.total_transaction_count, 0) as total_transaction_count,
    coalesce(i.total_deposit_amount, 0) as total_deposit_amount,
    coalesce(i.total_withdrawal_amount, 0) as total_withdrawal_amount,
    coalesce(i.estimated_net_balance, 0) as estimated_net_balance,
    coalesce(i.total_volume_processed, 0) as total_volume_processed,
    coalesce(i.avg_volume_amount, 0) as avg_volume_amount,
    coalesce(i.min_volume_amount, 0) as min_volume_amount,
    coalesce(i.max_volume_amount, 0) as max_volume_amount,

    i.first_transaction_at,
    i.last_transaction_at,
    i.day_since_last_transaction,

    coalesce(i.account_health_status, 'Inactive / New') as account_health_status,
    coalesce(i.customer_value_tier, 'zero_balance') as customer_value_tier,
    coalesce(i.is_overdrawn_flag, false) as is_overdrawn_flag

from staging_users u
left join intermediate_summary i
    on u.user_id = i.user_id