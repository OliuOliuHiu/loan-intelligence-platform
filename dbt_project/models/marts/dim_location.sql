-- {{ config(materialized='table', tags=['dim']) }}

-- select
--     row_number() over(order by address_state) as location_sk,
--     address_state
-- from {{ ref('stg_loans_union') }}
-- where is_deleted = false
-- group by address_state

{{ config(
    materialized = "incremental",
    unique_key = "location_sk",
    on_schema_change = "sync",
    tags = ['dim']
) }}

with source as (
    select
        {{ dbt_utils.generate_surrogate_key(['address_state']) }} as location_sk,
        address_state,
        current_timestamp as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current
    from {{ ref('stg_loans_union') }}
    where is_deleted = false and address_state is not null
    group by address_state
)

{% if is_incremental() %}

, new_records as (
    select s.*
    from source s
    left join {{ this }} d
      on s.location_sk = d.location_sk
     and d.is_current = true
    where d.location_sk is null
)

select * from new_records

-- Đóng dòng cũ nếu address_state đổi (nếu em bổ sung cột khác sau)
union all
select
    d.location_sk,
    d.address_state,
    d.valid_from,
    current_timestamp as valid_to,
    false as is_current
from {{ this }} d
join source s
  on s.location_sk = d.location_sk
where d.is_current = true
  and d.address_state != s.address_state

-- Thêm dòng mới thay thế
union all
select *
from source
where location_sk in (
    select d.location_sk
    from {{ this }} d
    join source s on s.location_sk = d.location_sk
    where d.is_current = true
      and d.address_state != s.address_state
)

{% else %}

-- Full refresh lần đầu
select * from source

{% endif %}

-- {{ log_load_info('dim_location') }}
-- {{ insert_audit_log('dim_location') }}