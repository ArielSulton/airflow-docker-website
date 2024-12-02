import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine

def generate_financial_transactions():
    np.random.seed(42)
    
    # Generate detailed transaction data
    transactions = {
        'transaction_id': range(1, 10001),
        'amount': np.random.uniform(10, 5000, 10000),
        'category': np.random.choice([
            'Makanan', 'Transportasi', 'Hiburan', 
            'Pendidikan', 'Kesehatan', 'Belanja',
            'Elektronik', 'Investasi'
        ], 10000),
        'merchant': np.random.choice([
            'Restoran A', 'Minimarket B', 'Toko Online C', 
            'Bengkel D', 'Bioskop E', 'Universitas F'
        ], 10000),
        'payment_method': np.random.choice([
            'Kartu Kredit', 'Transfer Bank', 'Tunai', 
            'E-Wallet', 'Debit Langsung'
        ], 10000),
        'timestamp': pd.date_range(
            start='2023-01-01', 
            end='2023-12-31', 
            periods=10000
        )
    }
    df = pd.DataFrame(transactions)
    return df

def store_to_postgresql(**kwargs):
    df = generate_financial_transactions()
    
    # Konfigurasi koneksi PostgreSQL
    engine = create_engine('postgresql://admin:rahasia@localhost:5432/financial_db')
    
    # Simpan ke tabel transactions
    df.to_sql('transactions', engine, if_exists='replace', index=False)

with DAG(
    'financial_data_generation',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily'
) as dag:
    generate_data = PythonOperator(
        task_id='generate_transactions',
        python_callable=generate_financial_transactions
    )

    store_data = PythonOperator(
        task_id='store_transactions',
        python_callable=store_to_postgresql
    )

    generate_data >> store_data