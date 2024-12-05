from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def analyze_daily_transactions():
    pg_hook = PostgresHook(postgres_conn_id='transactions_db')
    
    sql = """
        SELECT 
            DATE(timestamp) as date,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount,
            COUNT(DISTINCT merchant) as unique_merchants
        FROM transactions
        GROUP BY DATE(timestamp)
        ORDER BY date DESC
    """
    
    df = pd.read_sql_query(sql, pg_hook.get_uri())
    
    # Save analysis results
    pg_hook.insert_rows(
        table='transaction_analytics',
        rows=df.values.tolist(),
        target_fields=df.columns.tolist()
    )

def analyze_category_trends():
    pg_hook = PostgresHook(postgres_conn_id='transactions_db')
    
    sql = """
        SELECT 
            category,
            DATE(timestamp) as date,
            COUNT(*) as transaction_count,
            SUM(amount) as total_amount,
            AVG(amount) as avg_amount
        FROM transactions
        GROUP BY category, DATE(timestamp)
        ORDER BY date DESC, total_amount DESC
    """
    
    df = pd.read_sql_query(sql, pg_hook.get_uri())
    
    # Save category analysis
    pg_hook.insert_rows(
        table='category_analytics',
        rows=df.values.tolist(),
        target_fields=df.columns.tolist()
    )

with DAG(
    'transaction_analytics',
    default_args=default_args,
    description='Analyze transaction patterns',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    daily_analysis = PythonOperator(
        task_id='analyze_daily_transactions',
        python_callable=analyze_daily_transactions,
    )

    category_analysis = PythonOperator(
        task_id='analyze_category_trends',
        python_callable=analyze_category_trends,
    )

    daily_analysis >> category_analysis