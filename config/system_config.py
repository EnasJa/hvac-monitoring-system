"""
תצורת מערכת HVAC - הגדרות כלליות
"""
import os
from datetime import timedelta

class SystemConfig:
    # Basic Settings
    PROJECT_NAME = "Smart HVAC Monitoring System"
    VERSION = "1.0.0"
    DEBUG = True
    
    # Flask Settings
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'hvac-secret-key-2024'
    HOST = '0.0.0.0'
    PORT = 5000
    
    # Database Settings
    DATABASE_PATH = 'database/hvac_data.db'
    DATABASE_URL = f'sqlite:///{DATABASE_PATH}'
    
    # MQTT Settings
    MQTT_BROKER = 'localhost'
    MQTT_PORT = 1883
    MQTT_KEEPALIVE = 60
    MQTT_TOPICS = {
        'sensors': 'hvac/sensors/+/data',
        'alerts': 'hvac/alerts',
        'system': 'hvac/system'
    }
    
    # Sensor Locations
    SENSOR_LOCATIONS = {
        'office_a1': {
            'name': 'Office A1',
            'description': 'Main office floor 1',
            'coordinates': {'x': 10, 'y': 10},
            'capacity': 4
        },
        'office_a2': {
            'name': 'Office A2', 
            'description': 'Secondary office floor 1',
            'coordinates': {'x': 20, 'y': 10},
            'capacity': 4
        },
        'meeting_room': {
            'name': 'Meeting Room',
            'description': 'Main conference room',
            'coordinates': {'x': 30, 'y': 10},
            'capacity': 12
        },
        'main_corridor': {
            'name': 'Main Corridor',
            'description': 'Central hallway',
            'coordinates': {'x': 15, 'y': 20},
            'capacity': 0
        },
        'kitchen': {
            'name': 'Kitchen',
            'description': 'Employee kitchen',
            'coordinates': {'x': 5, 'y': 20},
            'capacity': 6
        },
        'reception': {
            'name': 'Reception',
            'description': 'Reception area',
            'coordinates': {'x': 25, 'y': 5},
            'capacity': 8
        },
        'restroom': {
            'name': 'Restroom',
            'description': 'Public restroom',
            'coordinates': {'x': 35, 'y': 20},
            'capacity': 0
        }
    }
    
    # ספי התראות
    THRESHOLDS = {
        'temperature': {
            'min': 18.0,
            'max': 26.0,
            'critical_min': 15.0,
            'critical_max': 30.0
        },
        'humidity': {
            'min': 30.0,
            'max': 60.0,
            'critical_min': 20.0,
            'critical_max': 80.0
        },
        'co2': {
            'max': 1000,
            'critical_max': 1500
        }
    }
    
    # הגדרות זיהוי חריגות
    ANOMALY_DETECTION = {
        'window_size': 50,  # גודל חלון לחישוב ממוצע
        'std_threshold': 2.5,  # סף סטיית תקן
        'min_samples': 10  # מינימום דגימות לאנליזה
    }
    
    # הגדרות זמן
    DATA_COLLECTION_INTERVAL = 5  # שניות
    ALERT_COOLDOWN = timedelta(minutes=5)  # זמן המתנה בין התראות
    DATA_RETENTION_DAYS = 365  # שמירת נתונים לשנה
    
    # הגדרות לוגים
    LOG_LEVEL = 'INFO'
    LOG_FILE = 'logs/hvac_system.log'
    LOG_FORMAT = '{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}'
    
    # הגדרות ביצועים
    MAX_CONCURRENT_CONNECTIONS = 100
    CACHE_TIMEOUT = 300  # 5 דקות
    
    # הגדרות התראות
    ALERT_LEVELS = {
        'LOW': {'priority': 1, 'color': '#28a745'},
        'MEDIUM': {'priority': 2, 'color': '#ffc107'},
        'HIGH': {'priority': 3, 'color': '#fd7e14'},
        'CRITICAL': {'priority': 4, 'color': '#dc3545'}
    }
    
    # הגדרות אנרגיה
    ENERGY_OPTIMIZATION = {
        'enable': True,
        'temperature_tolerance': 1.0,  # מעלה צלזיוס
        'humidity_tolerance': 5.0,     # אחוז
        'schedule': {
            'work_hours_start': '07:00',
            'work_hours_end': '19:00',
            'weekend_mode': True
        }
    }

class DevelopmentConfig(SystemConfig):
    """תצורת פיתוח"""
    DEBUG = True
    TESTING = False

class ProductionConfig(SystemConfig):
    """תצורת ייצור"""
    DEBUG = False
    TESTING = False
    LOG_LEVEL = 'WARNING'

class TestingConfig(SystemConfig):
    """תצורת בדיקות"""
    DEBUG = True
    TESTING = True
    DATABASE_PATH = 'database/test_hvac_data.db'

# בחירת תצורה לפי משתנה סביבה
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}

def get_config():
    """החזרת תצורה נוכחית"""
    env = os.environ.get('FLASK_ENV', 'default')
    return config.get(env, config['default'])