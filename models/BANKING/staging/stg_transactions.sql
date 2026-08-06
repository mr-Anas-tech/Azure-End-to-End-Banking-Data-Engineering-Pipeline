select
cast(transaction_id as string) as transaction_id,
cast(user_id as string) as user_id,
lower(trim(cast(transaction_type as string))) as transaction_type,
cast(amount as int) as amount,
cast(transaction_timestamp as date) as transaction_timestamp
from {{ source("db_bankingproject", "transactions")}}