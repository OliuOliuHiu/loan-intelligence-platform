-- {{ config(materialized='table', tags=['dim']) }}

-- select
--     row_number() over(order by term) as term_sk,
--     term
-- from {{ ref('stg_loans_union') }}
-- where is_deleted = false
-- group by term

{{ config(
    materialized = "incremental",
    unique_key = "term_sk",
    on_schema_change = "sync",
    tags = ['dim']
) }}

with source as (
    select
        {{ dbt_utils.generate_surrogate_key(['term']) }} as term_sk,
        term,
        current_timestamp as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current
    from {{ ref('stg_loans_union') }}
    where is_deleted = false and term is not null
    group by term
)

{% if is_incremental() %}

, new_records as (
    select s.*
    from source s
    left join {{ this }} d
      on s.term_sk = d.term_sk
     and d.is_current = true
    where d.term_sk is null
)

select * from new_records

-- Đóng dòng cũ nếu term đổi (hiếm khi xảy ra, nhưng cần SCD2 tracking)
union all
select
    d.term_sk,
    d.term,
    d.valid_from,
    current_timestamp as valid_to,
    false as is_current
from {{ this }} d
join source s
  on s.term_sk = d.term_sk
where d.is_current = true
  and d.term != s.term

-- Thêm dòng mới thay thế
union all
select *
from source
where term_sk in (
    select d.term_sk
    from {{ this }} d
    join source s on s.term_sk = d.term_sk
    where d.is_current = true
      and d.term != s.term
)

{% else %}

-- Lần đầu chạy: full-refresh
select * from source

{% endif %}

-- {{ log_load_info('dim_term') }}
-- {{ insert_audit_log('dim_term') }}