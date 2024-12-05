from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
from utils.data_generator import generate_financial_transactions
from utils.db_operations import get_database_engine, store_transactions_to_db

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_and_store_data():
    """Generate financial data and store it in the database"""
    try:
        df = generate_financial_transactions()
        engine = get_database_engine()
        store_transactions_to_db(df, engine)
    except Exception as e:
        raise AirflowFailException(f"Data generation or storage failed: {e}")

with DAG(
    'finance_data_generator',
    default_args=default_args,
    description='Generate and store synthetic financial data',
    # schedule_interval='@daily',
    schedule_interval='*/1 * * * *',
    # The five asterisks represent minute, hour, day of month, month, and day of week respectively
    catchup=False
) as dag:
    
    generate_store_task = PythonOperator(
        task_id='generate_and_store_data',
        python_callable=generate_and_store_data,
        dag=dag,
    )