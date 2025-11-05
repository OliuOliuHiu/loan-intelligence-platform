{{ config(
    materialized = "incremental",
    unique_key = "loan_id",
    on_schema_change = "sync"
) }}

select
    loan_id,
    member_id,
    grade,
    sub_grade,
    purpose,
    loan_status,
    term,
    verification_status,
    address_state,
    annual_income,
    loan_amount,
    int_rate,
    installment,
    total_acc,
    total_payment,
    dti,
    issue_date,
    last_payment_date,
    next_payment_date,
    last_credit_pull_date,
    last_updated,
    is_deleted
from {{ ref('stg_loans_union') }}
where is_deleted = false
{% if is_incremental() %}
  and last_updated > (select coalesce(max(last_updated), '1900-01-01') from {{ this }})
{% endif %}

-- {{ log_load_info('fact_base') }}
-- {{ insert_audit_log('fact_base') }}
