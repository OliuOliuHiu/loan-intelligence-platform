from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.email import send_email
from datetime import datetime
import os, pandas as pd, requests, pyodbc
from sqlalchemy import create_engine, text


DW_CONN_URI = os.environ["DW_CONN_URI"]

def ensure_raw_schema(engine):
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))

def extract_load_api():
    url = os.environ["SOURCE_API"]
    df = pd.DataFrame(requests.get(url).json())
    engine = create_engine(DW_CONN_URI)
    ensure_raw_schema(engine) 
    df.to_sql("api_loan_data", engine, schema="raw", if_exists="replace", index=False)

def extract_load_sqlserver():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.environ['SQLSERVER_HOST']},{os.environ['SQLSERVER_PORT']};"
        f"DATABASE={os.environ['SQLSERVER_DB']};"
        f"UID={os.environ['SQLSERVER_USER']};PWD={os.environ['SQLSERVER_PASSWORD']};"
        "TrustServerCertificate=yes"
    )
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql("SELECT * FROM dbo.loan_sql", conn)
    conn.close()

    engine = create_engine(DW_CONN_URI)
    ensure_raw_schema(engine) 
    df.to_sql("sqlsrv_loan_data", engine, schema="raw", if_exists="replace", index=False)

TEAM_EMAILS = [
    "hieuhd22416c@st.uel.edu.vn",
    "hieucm22416c@st.uel.edu.vn",
    "mybth22416c@st.uel.edu.vn",
    "mynhn22416c@st.uel.edu.vn",
    "anhnp22416c@st.uel.edu.vn"
]

def notify_failure(context):
    task = context.get("task_instance")
    subject = f"[Airflow Alert] Task {task.task_id} in {task.dag_id} Failed"
    body = f"""
    <h3>Airflow Task Failed</h3>
    <p><b>DAG:</b> {task.dag_id}</p>
    <p><b>Task:</b> {task.task_id}</p>
    <p><b>Execution Time:</b> {context.get('execution_date')}</p>
    <p><b>Log URL:</b> <a href="{task.log_url}">{task.log_url}</a></p>
    """
    send_email(to=TEAM_EMAILS, subject=subject, html_content=body)

def notify_success(context):
    dag_run = context.get("dag_run")
    subject = f"[Airflow Success] DAG {dag_run.dag_id} Completed Successfully"
    body = f"""
    <h3>Airflow DAG Completed Successfully </h3>
    <p><b>DAG:</b> {dag_run.dag_id}</p>
    <p><b>Execution Time:</b> {context.get('execution_date')}</p>
    <p>All tasks finished without errors.</p>
    """
    send_email(to=TEAM_EMAILS, subject=subject, html_content=body)

with DAG(
    dag_id="etl_multi_source",
    start_date=datetime(2025, 10, 28),
    schedule_interval= None,
    catchup=False,
    default_args={
    "email": TEAM_EMAILS,
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": notify_failure
    },
    on_success_callback=notify_success
) as dag:

    api_task = PythonOperator(task_id="extract_api", python_callable=extract_load_api)
    sqlsrv_task = PythonOperator(task_id="extract_sqlserver", python_callable=extract_load_sqlserver)

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt_project && dbt deps --profiles-dir profiles"
    )
    is_first_run = Variable.get("is_first_run", default_var="true")

    if is_first_run == "true":
        bash_cmd = (
            "cd /opt/airflow/dbt_project && "
            "dbt run --full-refresh --profiles-dir profiles && "
            "airflow variables set is_first_run false"
        )
    else:
        bash_cmd = "cd /opt/airflow/dbt_project && dbt run --profiles-dir profiles"
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command= bash_cmd
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command= "cd /opt/airflow/dbt_project && dbt test --profiles-dir profiles"
    )

    [api_task, sqlsrv_task] >> dbt_deps >> dbt_run >> dbt_test
