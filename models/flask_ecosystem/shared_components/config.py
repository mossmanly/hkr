#!/usr/bin/env python3
"""
Shared Configuration Settings for Flask Ecosystem
Extracted from HKR CoStar Parser
"""

import os

# Flask App Configuration
class BaseConfig:
    """Base configuration class with common settings"""
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-key-change-in-production')
    MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB max file size
    
    # Metabase configuration
    METABASE_DASHBOARD_URL = "https://hkh-metabase.onrender.com/public/dashboard/4e52af89-96ac-4284-a128-93d029acbbfb"
    METABASE_BASE_URL = "https://hkh-metabase.onrender.com"
    METABASE_API_KEY = None
    METABASE_DB_ID = 3

class DevelopmentConfig(BaseConfig):
    """Development environment configuration"""
    DEBUG = True
    
class ProductionConfig(BaseConfig):
    """Production environment configuration"""
    DEBUG = False

# Default to development
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}

def get_config(env='default'):
    """Get configuration for specified environment"""
    return config.get(env, config['default'])