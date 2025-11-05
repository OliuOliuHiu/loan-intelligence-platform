-- {{ config(materialized='table', tags=['dim']) }}

-- select
--     row_number() over(order by verification_status) as verification_sk,
--     verification_status
-- from {{ ref('stg_loans_union') }}
-- where is_deleted = false
-- group by verification_status

{{ config(
    materialized = "incremental",
    unique_key = "verification_sk",
    on_schema_change = "sync",
    tags = ['dim']
) }}

with source as (
    select
        {{ dbt_utils.generate_surrogate_key(['verification_status']) }} as verification_sk,
        verification_status,
        current_timestamp as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current
    from {{ ref('stg_loans_union') }}
    where is_deleted = false and verification_status is not null
    group by verification_status
)

{% if is_incremental() %}

, new_records as (
    select s.*
    from source s
    left join {{ this }} d
      on s.verification_sk = d.verification_sk
     and d.is_current = true
    where d.verification_sk is null
)

select * from new_records

-- Đóng dòng cũ nếu verification_status thay đổi
union all
select
    d.verification_sk,
    d.verification_status,
    d.valid_from,
    current_timestamp as valid_to,
    false as is_current
from {{ this }} d
join source s
  on s.verification_sk = d.verification_sk
where d.is_current = true
  and d.verification_status != s.verification_status

-- Thêm dòng mới thay thế
union all
select *
from source
where verification_sk in (
    select d.verification_sk
    from {{ this }} d
    join source s on s.verification_sk = d.verification_sk
    where d.is_current = true
      and d.verification_status != s.verification_status
)

{% else %}

-- Lần đầu chạy: full refresh
select * from source

{% endif %}

-- {{ log_load_info('dim_verification') }}
-- {{ insert_audit_log('dim_verification') }}