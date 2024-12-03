CREATE TABLE transactions (
    transaction_id INTEGER UNIQUE PRIMARY KEY,
    amount DECIMAL(10, 2),
    category VARCHAR(100),
    merchant VARCHAR(200),
    payment_method VARCHAR(100),
    timestamp TIMESTAMP
);