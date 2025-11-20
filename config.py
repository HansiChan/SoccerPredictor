# coding=utf-8
"""
Configuration file - centralize all configuration parameters
"""
import os

# Database configuration
DATABASE = {
    'DSN': os.getenv('IMPALA_DSN', 'Sample Cloudera Impala DSN'),
    'HOST': os.getenv('IMPALA_HOST', '192.168.3.191'),
    'PORT': os.getenv('IMPALA_PORT', '21050'),
    'UID': os.getenv('IMPALA_UID', 'hive'),
    'PWD': os.getenv('IMPALA_PWD', 'hive'),
    'AUTH_MECH': os.getenv('IMPALA_AUTH_MECH', '3'),
    'USE_SASL': os.getenv('IMPALA_USE_SASL', '0'),
    'TIMEOUT': int(os.getenv('IMPALA_TIMEOUT', '240'))
}

# Database table names
TABLES = {
    'TEAM_LIST': 'tmp.team_list',
    'GAME_RECORD': 'tmp.game_record',
    'GAME_RECORD_URL': 'tmp.game_record_url',
    'GAME_ODDS': 'tmp.game_odds',
    'GAME_OVERUNDER': 'tmp.game_overunder'
}

# Model configuration
MODEL = {
    'DIR': os.path.join(os.path.dirname(__file__), 'Models'),
    'TEST_SIZE': float(os.getenv('MODEL_TEST_SIZE', '0.6')),
    'RANDOM_STATE': int(os.getenv('MODEL_RANDOM_STATE', '2')),
    'MAX_DEPTH': int(os.getenv('MODEL_MAX_DEPTH', '2')),
    'N_ESTIMATORS': int(os.getenv('MODEL_N_ESTIMATORS', '100')),
    'LEARNING_RATE': float(os.getenv('MODEL_LEARNING_RATE', '0.1'))
}

# Spider configuration
SPIDER = {
    'HEADLESS': os.getenv('SPIDER_HEADLESS', 'True').lower() == 'true',
    'IMPLICIT_WAIT': int(os.getenv('SPIDER_IMPLICIT_WAIT', '30')),
    'PAGE_LOAD_TIMEOUT': int(os.getenv('SPIDER_PAGE_LOAD_TIMEOUT', '30'))
}

# Logging configuration
LOGGING = {
    'LEVEL': os.getenv('LOG_LEVEL', 'INFO'),
    'FORMAT': os.getenv('LOG_FORMAT', '%(asctime)s %(name)s %(levelname)s %(message)s')
}

def get_database_connection_string():
    """
    Generate database connection string
    :return: Connection string
    """
    return (f"DSN={DATABASE['DSN']};"
            f"HOST={DATABASE['HOST']};"
            f"PORT={DATABASE['PORT']};"
            f"UID={DATABASE['UID']};"
            f"AuthMech={DATABASE['AUTH_MECH']};"
            f"PWD={DATABASE['PWD']};"
            f"UseSasl={DATABASE['USE_SASL']}")
