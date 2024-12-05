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

def get_last_transaction_id(engine):
    """Get the last transaction_id from the database"""
    try:
        with engine.connect() as connection:
            result = connection.execute("SELECT MAX(transaction_id) FROM transactions")
            last_id = result.scalar()  # Fetch the maximum transaction_id
            return last_id if last_id else 0  # Return 0 if no transactions exist
    except SQLAlchemyError as e:
        print(f"Error fetching last transaction ID: {e}")
        raise

def update_transaction_ids(df, start_id):
    """Update transaction_id in the DataFrame starting from start_id"""
    df['transaction_id'] = range(start_id + 1, start_id + 1 + len(df))
    return df

def store_transactions_to_db(df, engine):
    """Store transactions DataFrame to PostgreSQL database with ID management"""
    try:
        # Get last transaction_id
        last_id = get_last_transaction_id(engine)
        
        # Update transaction_id in DataFrame
        df = update_transaction_ids(df, last_id)
        
        # Store updated DataFrame to database
        df.to_sql('transactions', engine, if_exists='append', index=False)
        print("Transactions successfully stored in database")
    except Exception as e:
        print(f"Error storing transactions: {e}")
        raise
