#!/usr/bin/env python3
"""
Shared Components Package for Flask Ecosystem
Phase I Foundation
"""

from .database import get_db_connection, create_analysis_run, DATABASE_CONFIG
from .config import get_config, BaseConfig
from .utils import trigger_dbt_refresh, trigger_metabase_refresh

__version__ = "1.0.0"
__all__ = [
    'get_db_connection', 
    'create_analysis_run', 
    'DATABASE_CONFIG',
    'get_config', 
    'BaseConfig',
    'trigger_dbt_refresh', 
    'trigger_metabase_refresh'
]