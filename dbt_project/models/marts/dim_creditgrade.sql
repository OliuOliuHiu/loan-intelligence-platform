-- {{ config(materialized='table', tags=['dim']) }}

-- select
--     row_number() over(order by grade, sub_grade) as creditgrade_sk,
--     grade,
--     sub_grade
-- from {{ ref('stg_loans_union') }}
-- where is_deleted = false
-- group by grade, sub_grade

{{ config(
    materialized = "incremental",
    unique_key = "creditgrade_sk",
    on_schema_change = "sync"
) }}

with source as (
    select
        {{ dbt_utils.generate_surrogate_key(['grade', 'sub_grade']) }} as creditgrade_sk,
        grade,
        sub_grade,
        current_timestamp as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current
    from {{ ref('stg_loans_union') }}
    where is_deleted = false
    group by grade, sub_grade
)

{% if is_incremental() %}

, new_records as (
    select s.*
    from source s
    left join {{ this }} d
      on s.creditgrade_sk = d.creditgrade_sk
     and d.is_current = true
    where d.creditgrade_sk is null
)

select * from new_records

-- Đóng dòng cũ nếu giá trị đã đổi
union all
select
    d.creditgrade_sk,
    d.grade,
    d.sub_grade,
    d.valid_from,
    current_timestamp as valid_to,
    false as is_current
from {{ this }} d
join source s
  on s.creditgrade_sk = d.creditgrade_sk
where d.is_current = true
  and (
    d.grade != s.grade or
    d.sub_grade != s.sub_grade
  )

-- Thêm dòng mới thay thế cho dòng bị đóng
union all
select *
from source
where creditgrade_sk in (
    select d.creditgrade_sk
    from {{ this }} d
    join source s on s.creditgrade_sk = d.creditgrade_sk
    where d.is_current = true
      and (
        d.grade != s.grade or
        d.sub_grade != s.sub_grade
      )
)

{% else %}

-- Nếu là lần đầu (full-refresh), chỉ insert toàn bộ từ source
select * from source

{% endif %}

-- {{ log_load_info('dim_creditgrade') }}
-- {{ insert_audit_log('dim_creditgrade') }}