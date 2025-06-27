#!/usr/bin/env python3
"""
Shared Utility Functions for Flask Ecosystem
Extracted from HKR CoStar Parser
"""

import subprocess
import time
import requests
import logging
from .config import BaseConfig

# Configure logging
logger = logging.getLogger(__name__)

def trigger_dbt_refresh(models="", timeout=300):
    """Run DBT models after data import"""
    try:
        print("Running DBT models...")
        
        cmd = ['dbt', 'run', '--profiles-dir', '.', '--project-dir', '.']
        if models:
            cmd.extend(['--models', models])
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        
        if result.returncode == 0:
            print("DBT models refreshed successfully")
            return True
        else:
            print(f"DBT error: {result.stderr}")
            print(f"DBT stdout: {result.stdout}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"DBT run timed out after {timeout/60} minutes")
        return False
    except Exception as e:
        print(f"DBT run failed: {e}")
        return False

def trigger_metabase_refresh():
    """Trigger Metabase to refresh data"""
    try:
        print("Triggering Metabase refresh...")
        
        # Give DBT time to finish completely
        time.sleep(5)
        
        config = BaseConfig()
        
        # If API key and DB ID are configured, use API
        if config.METABASE_API_KEY and config.METABASE_DB_ID:
            headers = {"X-Metabase-Session": config.METABASE_API_KEY}
            
            # Sync database schema
            sync_response = requests.post(
                f"{config.METABASE_BASE_URL}/api/database/{config.METABASE_DB_ID}/sync_schema",
                headers=headers,
                timeout=30
            )
            
            # Re-scan field values
            rescan_response = requests.post(
                f"{config.METABASE_BASE_URL}/api/database/{config.METABASE_DB_ID}/rescan_values",
                headers=headers,
                timeout=30
            )
            
            if sync_response.status_code == 200:
                print("Metabase schema sync triggered")
            else:
                print(f"Metabase schema sync failed: {sync_response.status_code}")
            
            return sync_response.status_code == 200
        else:
            print("Metabase API key or database ID not configured")
            return True
            
    except Exception as e:
        print(f"Metabase refresh failed: {e}")
        return True