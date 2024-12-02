from flask import Blueprint, render_template, jsonify
from sqlalchemy import func
from app import db
from app.models.transaction_model import Transaction
import pandas as pd
import plotly
import plotly.express as px
import json

analytics_bp = Blueprint('analytics', __name__)

@analytics_bp.route('/')
def dashboard():
    # Agregasi transaksi bulanan
    monthly_summary = db.session.query(
        func.date_trunc('month', Transaction.timestamp).label('month'),
        func.sum(Transaction.amount).label('total_amount'),
        func.count(Transaction.id).label('transaction_count')
    ).group_by('month').order_by('month').all()
    
    # Visualisasi dengan Plotly
    df_monthly = pd.DataFrame(monthly_summary, columns=['month', 'total_amount', 'transaction_count'])
    
    # Bar Chart Total Amount per Bulan
    fig_amount = px.bar(
        df_monthly, 
        x='month', 
        y='total_amount', 
        title='Total Transaksi Bulanan'
    )
    amount_chart_json = json.dumps(fig_amount, cls=plotly.utils.PlotlyJSONEncoder)
    
    # Pie Chart Kategori Transaksi
    category_summary = db.session.query(
        Transaction.category, 
        func.sum(Transaction.amount).label('total_amount')
    ).group_by(Transaction.category).all()
    
    df_category = pd.DataFrame(category_summary, columns=['category', 'total_amount'])
    
    fig_category = px.pie(
        df_category, 
        values='total_amount', 
        names='category', 
        title='Distribusi Transaksi per Kategori'
    )
    category_chart_json = json.dumps(fig_category, cls=plotly.utils.PlotlyJSONEncoder)
    
    return render_template(
        'dashboard.html', 
        amount_chart=amount_chart_json,
        category_chart=category_chart_json
    )