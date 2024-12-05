from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from app.utils.routes import dashboard_bp
import time
import os

db = SQLAlchemy()

def create_app():
    app = Flask(__name__)
    
    db_host = os.getenv('POSTGRES_HOST', 'postgres')
    db_port = os.getenv('POSTGRES_PORT', '5432')
    db_name = os.getenv('POSTGRES_DB', 'financial_db')
    db_user = os.getenv('POSTGRES_USER', 'admin')
    db_password = os.getenv('POSTGRES_PASSWORD', 'rahasia')
    
    app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ECHO'] = True

    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
            connection = engine.connect()
            connection.close()
            
            db.init_app(app)
            app.register_blueprint(dashboard_bp)
            
            with app.app_context():
                db.create_all()
            
            print("Database connection successful!")
            return app
        
        except OperationalError as e:
            retry_count += 1
            print(f"Gagal terhubung ke database: {e}. Percobaan {retry_count}/{max_retries}")
            time.sleep(5)
    
    raise Exception("Tidak dapat terhubung ke database setelah beberapa percobaan")

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=8080, debug=True)