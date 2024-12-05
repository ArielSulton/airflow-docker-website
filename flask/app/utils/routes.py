from flask import Blueprint, render_template, jsonify, request
from datetime import datetime, timedelta
from app.utils.models import Transaction

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/api/dashboard/data', methods=['GET'])
def get_dashboard_data():
    range_param = request.args.get('range', 'week')
    print(f"API Received range parameter: {range_param}")
    
    today = datetime.now()
    if range_param == 'week':
        start_date = today - timedelta(days=7)
    elif range_param == 'month':
        start_date = today - timedelta(days=30)
    elif range_param == 'quarter':
        start_date = today - timedelta(days=90)
    elif range_param == 'year':
        start_date = today - timedelta(days=365)
    else:
        start_date = today - timedelta(days=7)
        
    transactions = Transaction.query.filter(
        Transaction.timestamp >= start_date,
        Transaction.timestamp <= today
    ).all()
    
    print(f"Number of transactions retrieved: {len(transactions)}")
    print(f"Start Date: {start_date}, End Date: {today}")
    
    category_distribution = {
        'x': [],
        'y': [],
        'type': 'bar'
    }
    
    payment_methods = {
        'labels': [],
        'values': [],
        'type': 'pie'
    }
    
    daily_transactions = {
        'x': [],
        'y': [],
        'type': 'line'
    }
    
    categories = {}
    payments = {}
    daily = {}

    for t in transactions:
        date_str = t.timestamp.strftime('%Y-%m-%d')
        daily[date_str] = daily.get(date_str, 0) + t.amount
        categories[t.category] = categories.get(t.category, 0) + t.amount
        payments[t.payment_method] = payments.get(t.payment_method, 0) + t.amount

    sorted_categories = sorted(categories.items(), key=lambda item: item[1], reverse=True)
    category_distribution['x'] = [item[0] for item in sorted_categories]
    category_distribution['y'] = [item[1] for item in sorted_categories]

    payment_methods['labels'] = list(payments.keys())
    payment_methods['values'] = list(payments.values())
    
    daily_transactions['x'] = list(daily.keys())
    daily_transactions['y'] = list(daily.values())

    return jsonify({
        'category_distribution': [category_distribution],
        'payment_methods': [payment_methods],
        'daily_transactions': [daily_transactions],
        'summary': {
            'total_transactions': len(transactions),
            'total_amount': sum(t.amount for t in transactions),
            'average_transaction': sum(t.amount for t in transactions) / len(transactions) if transactions else 0
        }
    })

@dashboard_bp.route('/')
def dashboard():
    return render_template('dashboard.html')
