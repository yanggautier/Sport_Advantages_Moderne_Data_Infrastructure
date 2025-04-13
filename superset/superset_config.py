import os

# Import the custom secret key (generated during image build)
try:
    from superset.custom_secret_key import SECRET_KEY
except ImportError:
    # Fallback to environment variable
    SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY')
    if not SECRET_KEY or len(SECRET_KEY) < 32:
        # Use a hardcoded key if all else fails (not recommended for production)
        SECRET_KEY = 'thisISaLongAndVerySecretKeyUsedForTesting1234567890!@#$%^&*()'

# Database connection - using PostgreSQL instead of SQLite
SQLALCHEMY_DATABASE_URI = os.environ.get(
    'SUPERSET_DATABASE_URI', 
    'postgresql+psycopg2://superset:superset@superset-postgres:5432/superset'
)
SQLALCHEMY_TRACK_MODIFICATIONS = True

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True
# Add endpoints that need to be exempt from CSRF protection
WTF_CSRF_EXEMPT_LIST = ['superset.views.core.log']

# Superset specific config
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088

# Flask app builder settings
AUTH_TYPE = 1  # Database authentication
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Admin"

# Cache settings
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 60 * 60 * 24,  # 1 day default (in secs)
}

# Setup feature flags
FEATURE_FLAGS = {
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_NATIVE_FILTERS_SET': True,
    'ALERT_REPORTS': True,
    'ENABLE_TEMPLATE_PROCESSING': True,
    'EMBEDDED_SUPERSET': True,
}

# Additional settings
ENABLE_PROXY_FIX = True
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300

# Disable the warning for development key
SILENCE_FAB_SECURITY_WARNING = True

# Logging
ENABLE_TIME_ROTATE = True
FILENAME = os.path.join(os.path.dirname(__file__), "superset.log")
TIME_ROTATE_LOG_LEVEL = "DEBUG"
TIME_ROTATE_INTERVAL = 1
TIME_ROTATE_INTERVAL_TYPE = "days"