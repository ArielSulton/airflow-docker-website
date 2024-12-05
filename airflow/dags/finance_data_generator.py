from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
from utils.data_generator import generate_financial_transactions
from utils.db_operations import get_database_engine, get_last_transaction_id, store_transactions_to_db, update_transaction_ids

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
        # Step 1: Initialize database engine
        engine = get_database_engine()
        
        # Step 2: Get the last transaction_id
        last_id = get_last_transaction_id(engine)
        
        # Step 3: Generate financial transactions
        size = 100  # Number of transactions to generate
        df = generate_financial_transactions(size=size)
        
        # Step 4: Update transaction_id starting from last_id
        df = update_transaction_ids(df, last_id)
        
        # Step 5: Store transactions in the database
        store_transactions_to_db(df, engine)
    except Exception as e:
        raise AirflowFailException(f"Data generation or storage failed: {e}")

with DAG(
    'finance_data_generator',
    default_args=default_args,
    description='Generate and store synthetic financial data',
    schedule_interval='*/1 * * * *',
    catchup=False
) as dag:
    
    generate_store_task = PythonOperator(
        task_id='generate_and_store_data',
        python_callable=generate_and_store_data,
    )