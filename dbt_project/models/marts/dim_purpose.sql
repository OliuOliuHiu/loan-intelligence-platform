-- {{ config(materialized='table', tags=['dim']) }}

-- select
--     row_number() over(order by purpose) as purpose_sk,
--     purpose
-- from {{ ref('stg_loans_union') }}
-- where is_deleted = false
-- group by purpose


{{ config(
    materialized = "incremental",
    unique_key = "purpose_sk",
    on_schema_change = "sync",
    tags = ['dim']
) }}

with source as (
    select
        {{ dbt_utils.generate_surrogate_key(['purpose']) }} as purpose_sk,
        purpose,
        current_timestamp as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current
    from {{ ref('stg_loans_union') }}
    where is_deleted = false and purpose is not null
    group by purpose
)

{% if is_incremental() %}

, new_records as (
    select s.*
    from source s
    left join {{ this }} d
      on s.purpose_sk = d.purpose_sk
     and d.is_current = true
    where d.purpose_sk is null
)

select * from new_records

-- Đóng dòng cũ nếu purpose thay đổi (hiếm nhưng vẫn cần kiểm tra)
union all
select
    d.purpose_sk,
    d.purpose,
    d.valid_from,
    current_timestamp as valid_to,
    false as is_current
from {{ this }} d
join source s
  on s.purpose_sk = d.purpose_sk
where d.is_current = true
  and d.purpose != s.purpose

-- Thêm dòng mới
union all
select *
from source
where purpose_sk in (
    select d.purpose_sk
    from {{ this }} d
    join source s on s.purpose_sk = d.purpose_sk
    where d.is_current = true
      and d.purpose != s.purpose
)

{% else %}

-- Lần đầu chạy: full refresh
select * from source

{% endif %}

-- {{ log_load_info('dim_purpose') }}
-- {{ insert_audit_log('dim_purpose') }}