{{
    config(
        materialized = "incremental",
        unique_key = "loan_id",
        on_schema_change = "sync"
    )
}}

with sqlsrv_source as (
    select
        cast(id as varchar) as loan_id,
        address_state,
        application_type,
        emp_length,
        emp_title,
        grade,  
        home_ownership,

        -- issue_date: bị YYYY-DD-MM → đảo lại thành YYYY-MM-DD
        TO_TIMESTAMP(
            CONCAT(
                SUBSTRING(issue_date::text, 1, 4), '-',  -- YYYY
                SUBSTRING(issue_date::text, 9, 2), '-',  -- MM
                SUBSTRING(issue_date::text, 6, 2)        -- DD
            ),
            'YYYY-MM-DD'
        ) as issue_date,

        -- Các cột sau là MM/DD/YYYY (chuẩn SQL Server)
        TO_TIMESTAMP(last_credit_pull_date, 'MM/DD/YYYY') as last_credit_pull_date,
        TO_TIMESTAMP(last_payment_date, 'MM/DD/YYYY') as last_payment_date,
        TO_TIMESTAMP(next_payment_date, 'MM/DD/YYYY') as next_payment_date,

        -- Đã là timestamp chuẩn
        last_updated,

        loan_status,
        cast(member_id as varchar) as member_id,
        purpose,
        sub_grade,
        term,
        verification_status,

        -- Chuẩn hoá kiểu số
        cast(replace(cast(annual_income as text), ',', '') as float) as annual_income,
        cast(replace(cast(dti as text), ',', '') as float) as dti,
        cast(replace(cast(installment as text), ',', '') as float) as installment,
        cast(replace(cast(int_rate as text), ',', '') as float) as int_rate,
        cast(replace(cast(loan_amount as text), ',', '') as float) as loan_amount,
        cast(replace(cast(total_acc as text), ',', '') as float) as total_acc,
        cast(replace(cast(total_payment as text), ',', '') as float) as total_payment,

        case
            when cast(is_deleted as text) in ('1','true','True','t','T') then true
            else false
        end as is_deleted,

        'sqlserver' as source_system
    from {{ source('sqlsrv', 'sqlsrv_loan_data') }}
),

api_source as (
    select
        cast(id as varchar) as loan_id,
        address_state,
        application_type,
        emp_length,
        emp_title,
        grade,
        home_ownership,

        -- Tất cả 4 cột đều dạng DD-MM-YYYY
        TO_TIMESTAMP(issue_date, 'DD-MM-YYYY') as issue_date,
        TO_TIMESTAMP(last_credit_pull_date, 'DD-MM-YYYY') as last_credit_pull_date,
        TO_TIMESTAMP(last_payment_date, 'DD-MM-YYYY') as last_payment_date,
        TO_TIMESTAMP(next_payment_date, 'DD-MM-YYYY') as next_payment_date,

        -- last_updated trong API là DD-MM-YYYY
        TO_TIMESTAMP(last_updated, 'DD-MM-YYYY') as last_updated,

        loan_status,
        cast(member_id as varchar) as member_id,
        purpose,
        sub_grade,
        term,
        verification_status,

        cast(replace(cast(annual_income as text), ',', '') as float) as annual_income,
        cast(replace(cast(dti as text), ',', '') as float) as dti,
        cast(replace(cast(installment as text), ',', '') as float) as installment,
        cast(replace(cast(int_rate as text), ',', '') as float) as int_rate,
        cast(replace(cast(loan_amount as text), ',', '') as float) as loan_amount,
        cast(replace(cast(total_acc as text), ',', '') as float) as total_acc,
        cast(replace(cast(total_payment as text), ',', '') as float) as total_payment,

        case
            when cast(is_deleted as text) in ('1','true','True','t','T') then true
            else false
        end as is_deleted,

        'api' as source_system
    from {{ source('api', 'api_loan_data') }}
),

unioned as (
    select * from sqlsrv_source
    union all
    select * from api_source
)

select *
from unioned
{% if is_incremental() %}
  where last_updated > (select coalesce(max(last_updated), '1900-01-01') from {{ this }})
{% endif %}

-- {{ log_load_info('stg_loans_union') }}