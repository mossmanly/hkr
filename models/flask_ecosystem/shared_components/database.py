#!/usr/bin/env python3
"""
Shared Database Configuration and Connection Management
Extracted from HKR CoStar Parser for Flask Ecosystem
"""

import psycopg2
import time
import os
from datetime import datetime

# Database configuration - centralized for all apps
DATABASE_CONFIG = {
    'host': 'dpg-d0glfhjuibrs73fnvht0-a.oregon-postgres.render.com',
    'database': 'hkh_decision_support_db',
    'user': 'moss',
    'password': 'zhotIlS6rAjOitkOfPuEZ8hnoL93XLVF',
    'port': '5432',
    'sslmode': 'require'
}

def get_db_connection():
    """Create a fresh database connection with retry logic"""
    for attempt in range(3):
        try:
            print(f"Creating database connection... (attempt {attempt + 1})")
            conn = psycopg2.connect(**DATABASE_CONFIG)
            print("Database connection established")
            return conn
        except Exception as e:
            print(f"Database connection attempt {attempt + 1} failed: {e}")
            if attempt < 2:
                print("Retrying in 3 seconds...")
                time.sleep(3)
            else:
                print("All connection attempts failed")
                raise

def create_analysis_run(notes="", app_name="generic"):
    """Create a new analysis run record and return the run_id"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            print("Creating analysis run...")
            timestamp = datetime.now()
            unique_suffix = f"{timestamp.strftime('%Y%m%d_%H%M%S')}_{timestamp.microsecond}"
            
            cur.execute("""
                INSERT INTO costar_analysis.analysis_runs 
                (run_name, upload_filename, upload_batch_id, run_status, started_at, notes)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
                RETURNING id
            """, (
                f"{app_name}_Analysis_{unique_suffix}",
                f"{unique_suffix}_upload.xlsx",
                f"batch_{unique_suffix}",
                'processing',
                notes
            ))
            
            run_id = cur.fetchone()[0]
            conn.commit()
            print(f"Created analysis run with ID: {run_id}")
            return run_id
    finally:
        conn.close()