{{ config(
    materialized = 'incremental',
    unique_key = 'loan_id',
    on_schema_change = 'sync'
) }}

{% if is_incremental() %}
    with latest as (
        select coalesce(max(last_updated), '1900-01-01') as max_last_updated
        from {{ this }}
    ),
{% else %}
    with latest as (
        select cast('1900-01-01' as timestamp) as max_last_updated
    ),
{% endif %}

f as (
    select *
    from {{ ref('fact_base') }} fb
    where fb.is_deleted = false
    {% if is_incremental() %}
      and fb.last_updated > (select max_last_updated from latest)
    {% endif %}
),

dc as ( select customer_sk, customer_id from {{ ref('dim_customer') }} where is_current = true ),
dp as ( select purpose_sk, purpose from {{ ref('dim_purpose') }} where is_current = true ),
dg as ( select creditgrade_sk, grade, sub_grade from {{ ref('dim_creditgrade') }} where is_current = true ),
ds as ( select loanstatus_sk, loan_status from {{ ref('dim_loanstatus') }} where is_current = true ),
dt as ( select term_sk, term from {{ ref('dim_term') }} where is_current = true ),
dv as ( select verification_sk, verification_status from {{ ref('dim_verification') }} where is_current = true ),
dl as ( select location_sk, address_state from {{ ref('dim_location') }} where is_current = true )

select
    f.loan_id,
    dc.customer_sk,
    dp.purpose_sk,
    dg.creditgrade_sk,
    ds.loanstatus_sk,
    dt.term_sk,
    dv.verification_sk,
    dl.location_sk,
    f.annual_income,
    f.loan_amount,
    f.int_rate,
    f.installment,
    f.total_payment,
    f.total_acc,
    f.dti,
    f.issue_date,
    f.last_payment_date,
    f.next_payment_date,
    f.last_updated
from f
left join dc on cast(f.member_id as varchar) = dc.customer_id
left join dp on f.purpose = dp.purpose
left join dg on f.grade = dg.grade and f.sub_grade = dg.sub_grade
left join ds on f.loan_status = ds.loan_status
left join dt on f.term = dt.term
left join dv on f.verification_status = dv.verification_status
left join dl on f.address_state = dl.address_state

-- {{ log_load_info('fact_loan') }}
-- {{ insert_audit_log('fact_loan') }}