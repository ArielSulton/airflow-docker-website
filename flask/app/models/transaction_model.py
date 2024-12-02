from app import db
from sqlalchemy import DateTime, Float, String, Integer

class Transaction(db.Model):
    __tablename__ = 'transactions'
    
    id = db.Column(Integer, primary_key=True)
    transaction_id = db.Column(Integer, unique=True)
    amount = db.Column(Float)
    category = db.Column(String)
    merchant = db.Column(String)
    payment_method = db.Column(String)
    timestamp = db.Column(DateTime)