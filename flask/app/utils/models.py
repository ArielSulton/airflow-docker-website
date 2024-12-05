from datetime import datetime
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Transaction(db.Model):
    __tablename__ = 'transactions'
    
    transaction_id = db.Column(db.Integer, unique=True, primary_key=True)
    amount = db.Column(db.Numeric(10, 2))
    category = db.Column(db.String(100))
    merchant = db.Column(db.String(200))
    payment_method = db.Column(db.String(100))
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'transaction_id': self.transaction_id,
            'amount': float(self.amount),
            'category': self.category,
            'merchant': self.merchant,
            'payment_method': self.payment_method,
            'timestamp': self.timestamp.isoformat()
        }