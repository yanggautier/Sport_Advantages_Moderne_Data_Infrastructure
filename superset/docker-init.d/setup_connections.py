#!/usr/bin/env python
import os
import time
import sys
from sqlalchemy import create_engine

# Ajout du chemin de l'application au path
sys.path.append('/app')

try:
    # Allow time for database to be ready
    time.sleep(10)
    
    from flask import current_app
    from superset.app import create_app
    
    app = create_app()
    
    with app.app_context():
        from superset import db
        from superset.models.core import Database
        
        # Check if database connection already exists
        db_engine = db.session.query(Database).filter_by(database_name='SportAdvantages').first()
        
        if not db_engine:
            # Get database configuration from environment variables
            db_user = os.environ.get('POSTGRES_USER')
            db_password = os.environ.get('POSTGRES_PASSWORD')
            db_name = os.environ.get('POSTGRES_DB')
            db_host = os.environ.get('POSTGRES_HOST')
            db_port = os.environ.get('POSTGRES_PORT')
            
            # Create connection string
            sqlalchemy_uri = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
            
            # Add new database connection
            new_db = Database(
                database_name='SportAdvantages',
                sqlalchemy_uri=sqlalchemy_uri,
                cache_timeout=0,
                expose_in_sqllab=True,
                allow_run_async=True,
                allow_ctas=True,
                allow_cvas=True,
                allow_dml=True,
                allow_multi_schema_metadata_fetch=True,
            )
            db.session.add(new_db)
            db.session.commit()
            print("Added database connection: SportAdvantages")
        else:
            print("Database connection already exists: SportAdvantages")
            
        # Check/add Trino connection if needed
        trino_engine = db.session.query(Database).filter_by(database_name='Trino').first()
        if not trino_engine:
            trino_host = os.environ.get('TRINO_HOST', 'trino')
            trino_port = os.environ.get('TRINO_PORT', '8080')
            trino_user = os.environ.get('TRINO_USER', 'trino')
            trino_catalogs = os.environ.get('TRINO_CATALOG', 'postgresql,minio')
            
            # Create connection string for Trino
            trino_uri = f'trino://{trino_user}@{trino_host}:{trino_port}'
            
            # Add new Trino connection
            new_trino = Database(
                database_name='Trino',
                sqlalchemy_uri=trino_uri,
                cache_timeout=0,
                expose_in_sqllab=True,
                allow_run_async=True,
                allow_ctas=True,
                allow_cvas=True,
                allow_dml=True,
                allow_multi_schema_metadata_fetch=True,
            )
            db.session.add(new_trino)
            db.session.commit()
            print("Added database connection: Trino")
            
except Exception as e:
    print(f"Error setting up database connection: {e}")
    import traceback
    traceback.print_exc()