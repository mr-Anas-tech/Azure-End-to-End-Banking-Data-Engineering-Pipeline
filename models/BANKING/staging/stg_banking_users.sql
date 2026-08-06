select
cast(login_uuid as string) as user_id,
cast(id_value as string) as national_id,
cast(id_name as string) as nation_id_type,

coalesce(cast(name_title as string), 'Not_provided') as title,
coalesce(cast(concat(name_first,' ', name_last) as string), 'Unknown') as name,
cast(gender as string) as gender,
cast(nat as string) as nationality,

cast(email as string) as email,
cast(cell as string) as cell_number,
cast(phone as string) as phone_number,

coalesce(cast(street_name as string), 'Unknown') as street_name,
cast(street_number as string) as street_number,
coalesce(cast(city as string), 'Not_provided') as city,
coalesce(cast(state as string), 'Unknown') as user_state,
coalesce(cast(country as string), 'Unknown') as country,
cast(postcode as string) as postal_code,
cast(coordinate_latitude as string) as coordinate_latitude,
cast(coordinate_longitude as string) as coordinate_longitude,
coalesce(cast(timezone_description as string), 'Not_provided') as timezone_description,
cast(timezone_offset as string) as timezone_offset,

cast(dob_date as date) as date_of_birth,
cast(dob_age as int) as age,
cast(registered_date as date) as registration_date,
cast(registered_age as int) as account_age_year,

cast(login_username as string) as username,
cast(login_password as string) as password,
cast(login_salt as string) as password_salt,
cast(login_md5 as string) as md5_hash,

cast(picture_large as string) as profile_pic_large,
cast(picture_medium as string) as profile_pic_medium,
cast(picture_thumbnail as string) as profile_pic_thumbnail
from {{ source("db_bankingproject", "raw_banking")}}