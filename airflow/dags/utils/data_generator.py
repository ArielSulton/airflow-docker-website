import numpy as np
import pandas as pd

def generate_transaction_ids(size):
    return range(1, size + 1)

def generate_amounts(size):
    np.random.seed(42)
    return np.random.uniform(10, 5000, size)

def generate_categories(size):
    categories = [
        'Food', 'Transportation', 'Entertainment', 
        'Education', 'Health', 'Shopping',
        'Electronics', 'Investment'
    ]
    return np.random.choice(categories, size)

def generate_merchants(size):
    merchants = [
        'Restaurant A', 'Minimarket B', 'Online Store C', 
        'Workshop D', 'Cinema E', 'University F'
    ]
    return np.random.choice(merchants, size)

def generate_payment_methods(size):
    methods = [
        'Credit Card', 'Bank Transfer', 'Cash', 
        'E-Wallet', 'Debit Card'
    ]
    return np.random.choice(methods, size)

def generate_timestamps(size):
    return pd.date_range(
        start='2024-01-01',
        end='2024-12-31',
        periods=size
    )

def generate_financial_transactions(size=100):
    """Generate synthetic financial transaction data"""
    transactions = {
        'transaction_id': generate_transaction_ids(size),
        'amount': generate_amounts(size),
        'category': generate_categories(size),
        'merchant': generate_merchants(size),
        'payment_method': generate_payment_methods(size),
        'timestamp': generate_timestamps(size)
    }
    return pd.DataFrame(transactions)