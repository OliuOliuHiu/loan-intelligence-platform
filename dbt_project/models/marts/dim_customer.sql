-- {{ config(materialized='table', tags=['dim']) }}

-- select
--     row_number() over(order by member_id) as customer_sk,
--     cast(member_id as varchar) as customer_id,
--     emp_title,
--     emp_length,
--     home_ownership
-- from {{ ref('stg_loans_union') }}
-- where is_deleted = false
-- group by member_id, emp_title, emp_length, home_ownership

{{ config(
    materialized = "incremental",
    unique_key = "customer_sk",
    on_schema_change = "sync",
    tags = ['dim']
) }}

with source as (
    select
        {{ dbt_utils.generate_surrogate_key(['member_id']) }} as customer_sk,
        cast(member_id as varchar) as customer_id,
        emp_title,
        emp_length,
        home_ownership,
        current_timestamp as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current
    from {{ ref('stg_loans_union') }}
    where is_deleted = false and member_id is not null
    group by member_id, emp_title, emp_length, home_ownership
)

{% if is_incremental() %}

, new_records as (
    select s.*
    from source s
    left join {{ this }} d
      on s.customer_sk = d.customer_sk
     and d.is_current = true
    where d.customer_sk is null
)

select * from new_records

-- Đóng dòng cũ nếu thông tin thay đổi
union all
select
    d.customer_sk,
    d.customer_id,
    d.emp_title,
    d.emp_length,
    d.home_ownership,
    d.valid_from,
    current_timestamp as valid_to,
    false as is_current
from {{ this }} d
join source s
  on s.customer_sk = d.customer_sk
where d.is_current = true
  and (
    d.emp_title != s.emp_title or
    d.emp_length != s.emp_length or
    d.home_ownership != s.home_ownership
  )

-- Thêm dòng mới thay thế
union all
select *
from source
where customer_sk in (
    select d.customer_sk
    from {{ this }} d
    join source s on s.customer_sk = d.customer_sk
    where d.is_current = true
      and (
        d.emp_title != s.emp_title or
        d.emp_length != s.emp_length or
        d.home_ownership != s.home_ownership
      )
)

{% else %}

-- Lần đầu chạy full-refresh
select * from source

{% endif %}


-- {{ log_load_info('dim_customer') }}
-- {{ insert_audit_log('dim_customer') }}
