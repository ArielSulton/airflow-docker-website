from flask import Flask
from flask_sqlalchemy import SQLAlchemy
# from flask_migrate import Migrate
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import time
import os

db = SQLAlchemy()
migrate = None

def create_app():
    global migrate
    app = Flask(__name__)
    
    # Konfigurasi database dengan environment variable
    db_host = os.getenv('POSTGRES_HOST', 'localhost')
    db_port = os.getenv('POSTGRES_PORT', '5432')
    db_name = os.getenv('POSTGRES_DB', 'financial_db')
    db_user = os.getenv('POSTGRES_USER', 'admin')
    db_password = os.getenv('POSTGRES_PASSWORD', 'rahasia')
    
    # Konfigurasi database URI
    app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # Retry mechanism untuk koneksi database
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Coba buat koneksi
            engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
            connection = engine.connect()
            connection.close()
            
            # Inisialisasi database dan migrate
            db.init_app(app)
            # migrate = Migrate(app, db)
            
            # Import dan daftarkan blueprint
            from .routes.analytics_routes import analytics_bp
            app.register_blueprint(analytics_bp)
            
            print("Database connection successful!")
            return app
        
        except OperationalError as e:
            retry_count += 1
            print(f"Gagal terhubung ke database: {e}. Percobaan {retry_count}/{max_retries}")
            time.sleep(5)
    
    raise Exception("Tidak dapat terhubung ke database setelah beberapa percobaan")