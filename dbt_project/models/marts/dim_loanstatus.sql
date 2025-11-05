-- {{ config(materialized='table', tags=['dim']) }}

-- select
--     row_number() over(order by loan_status) as loanstatus_sk,
--     loan_status,
--     case 
--         when loan_status in ('Fully Paid', 'Current') then 'Good Loan'
--         when loan_status = 'Charged Off' then 'Bad Loan'
--         else 'Other'
--     end as loan_status_category
-- from {{ ref('stg_loans_union') }}
-- where is_deleted = false
-- group by loan_status

{{ config(
    materialized = "incremental",
    unique_key = "loanstatus_sk",
    on_schema_change = "sync",
    tags = ['dim']
) }}

with source as (
    select
        {{ dbt_utils.generate_surrogate_key(['loan_status']) }} as loanstatus_sk,
        loan_status,
        case 
            when loan_status in ('Fully Paid', 'Current') then 'Good Loan'
            when loan_status = 'Charged Off' then 'Bad Loan'
            else 'Other'
        end as loan_status_category,
        current_timestamp as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current
    from {{ ref('stg_loans_union') }}
    where is_deleted = false and loan_status is not null
    group by loan_status
)

{% if is_incremental() %}

, new_records as (
    select s.*
    from source s
    left join {{ this }} d
      on s.loanstatus_sk = d.loanstatus_sk
     and d.is_current = true
    where d.loanstatus_sk is null
)

select * from new_records

-- Đóng dòng cũ nếu thay đổi category
union all
select
    d.loanstatus_sk,
    d.loan_status,
    d.loan_status_category,
    d.valid_from,
    current_timestamp as valid_to,
    false as is_current
from {{ this }} d
join source s
  on s.loanstatus_sk = d.loanstatus_sk
where d.is_current = true
  and d.loan_status_category != s.loan_status_category

-- Thêm dòng mới
union all
select *
from source
where loanstatus_sk in (
    select d.loanstatus_sk
    from {{ this }} d
    join source s on s.loanstatus_sk = d.loanstatus_sk
    where d.is_current = true
      and d.loan_status_category != s.loan_status_category
)

{% else %}

-- Full refresh lần đầu
select * from source

{% endif %}


-- {{ log_load_info('dim_loanstatus') }}
-- {{ insert_audit_log('dim_loanstatus') }}