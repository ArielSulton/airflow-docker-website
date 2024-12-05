import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def get_database_engine():
    """Create and return database engine with robust error handling"""
    db_host = os.getenv('POSTGRES_HOST', 'postgres')
    db_port = os.getenv('POSTGRES_PORT', '5432')
    db_name = os.getenv('POSTGRES_DB', 'financial_db')
    db_user = os.getenv('POSTGRES_USER', 'admin')
    db_password = os.getenv('POSTGRES_PASSWORD', 'rahasia')
    
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    
    try:
        engine = create_engine(connection_string, pool_pre_ping=True)
        # Test connection
        with engine.connect() as connection:
            connection.execute('SELECT 1')
        return engine
    except SQLAlchemyError as e:
        print(f"Database connection error: {e}")
        raise

def store_transactions_to_db(df, engine):
    """Store transactions DataFrame to PostgreSQL database with error handling"""
    try:
        df.to_sql('transactions', engine, if_exists='replace', index=False)
        print("Transactions successfully stored in database")
    except Exception as e:
        print(f"Error storing transactions: {e}")
        raise