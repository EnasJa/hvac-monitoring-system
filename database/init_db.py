#!/usr/bin/env python3
"""
Database initialization script for Smart HVAC Monitoring System
Creates all necessary tables and initial data
"""
import os
import sys
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.models import DatabaseManager, Base
from config.system_config import SystemConfig

def create_directories():
    """Create necessary directories"""
    directories = [
        'database',
        'logs',
        'web_dashboard/static/css',
        'web_dashboard/static/js',
        'web_dashboard/templates'
    ]
    
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"✅ Created directory: {directory}")
        else:
            print(f"📁 Directory exists: {directory}")

def initialize_database():
    """Initialize the database with tables and initial data"""
    print("🗄️ Initializing Smart HVAC Database...")
    
    try:
        # Create database manager
        db_manager = DatabaseManager()
        
        # Initialize sensors
        db_manager.initialize_sensors()
        
        print("✅ Database initialized successfully")
        return True
        
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return False

def create_sample_data():
    """Create sample data for testing"""
    print("📊 Creating sample data...")
    
    try:
        from datetime import datetime, timedelta
        import random
        
        db_manager = DatabaseManager()
        
        # Sample sensor readings
        sample_readings = []
        base_time = datetime.now() - timedelta(hours=24)
        
        config = SystemConfig()
        sensor_locations = list(config.SENSOR_LOCATIONS.keys())
        
        for i in range(100):  # 100 sample readings
            for location_id in sensor_locations:
                sensor_id = f"hvac_{location_id}"
                timestamp = base_time + timedelta(minutes=i * 5)
                
                # Generate realistic sample data
                temperature = random.uniform(20, 26) + random.uniform(-2, 2)
                humidity = random.uniform(40, 60) + random.uniform(-5, 5)
                co2 = random.randint(400, 800) + random.randint(-50, 100)
                occupancy = random.randint(0, config.SENSOR_LOCATIONS[location_id]['capacity'])
                
                reading_data = {
                    'sensor_id': sensor_id,
                    'timestamp': timestamp,
                    'temperature': round(temperature, 1),
                    'humidity': round(humidity, 1),
                    'co2': co2,
                    'air_quality_index': random.uniform(70, 95),
                    'occupancy': occupancy,
                    'system_status': random.choice(['NORMAL', 'NORMAL', 'NORMAL', 'WARNING'])
                }
                
                sample_readings.append(reading_data)
        
        # Insert sample readings
        for reading_data in sample_readings:
            db_manager.save_sensor_reading(reading_data)
        
        print(f"✅ Created {len(sample_readings)} sample readings")
        
        # Sample alerts
        sample_alerts = [
            {
                'alert_id': 'alert_001',
                'sensor_id': 'hvac_office_a1',
                'alert_type': 'TEMPERATURE_HIGH',
                'severity': 'MEDIUM',
                'message': 'Temperature above normal range',
                'values': {'temperature': 28.5},
                'recommended_action': 'Increase cooling',
                'timestamp': datetime.now() - timedelta(hours=2)
            },
            {
                'alert_id': 'alert_002',
                'sensor_id': 'hvac_meeting_room',
                'alert_type': 'CO2_HIGH',
                'severity': 'HIGH',
                'message': 'CO₂ levels elevated',
                'values': {'co2': 1200},
                'recommended_action': 'Increase ventilation',
                'timestamp': datetime.now() - timedelta(minutes=30)
            }
        ]
        
        for alert_data in sample_alerts:
            db_manager.save_alert(alert_data)
        
        print(f"✅ Created {len(sample_alerts)} sample alerts")
        return True
        
    except Exception as e:
        print(f"❌ Sample data creation failed: {e}")
        return False

def verify_installation():
    """Verify that everything is installed correctly"""
    print("🔍 Verifying installation...")
    
    try:
        db_manager = DatabaseManager()
        
        # Check tables exist
        session = db_manager.get_session()
        
        # Test basic operations
        from database.models import Sensor, SensorReading, Alert
        
        sensor_count = session.query(Sensor).count()
        reading_count = session.query(SensorReading).count()
        alert_count = session.query(Alert).count()
        
        session.close()
        
        print(f"📊 Database Statistics:")
        print(f"   Sensors: {sensor_count}")
        print(f"   Readings: {reading_count}")
        print(f"   Alerts: {alert_count}")
        
        if sensor_count >= 7:
            print("✅ Installation verified successfully")
            return True
        else:
            print("⚠️ Warning: Expected at least 7 sensors")
            return False
            
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return False

def main():
    """Main initialization function"""
    print("""
╔══════════════════════════════════════════════════════════════════╗
║               Smart HVAC Database Initialization                 ║
║                         Version 1.0.0                           ║
╚══════════════════════════════════════════════════════════════════╝
    """)
    
    # Step 1: Create directories
    print("\n🔧 Step 1: Creating directories...")
    create_directories()
    
    # Step 2: Initialize database
    print("\n🔧 Step 2: Initializing database...")
    if not initialize_database():
        print("❌ Database initialization failed. Exiting.")
        sys.exit(1)
    
    # Step 3: Create sample data (optional)
    create_sample = input("\n❓ Create sample data for testing? (y/N): ").lower().strip()
    if create_sample in ['y', 'yes']:
        print("\n🔧 Step 3: Creating sample data...")
        create_sample_data()
    else:
        print("\n⏩ Step 3: Skipped sample data creation")
    
    # Step 4: Verify installation
    print("\n🔧 Step 4: Verifying installation...")
    if verify_installation():
        print(f"""
╔══════════════════════════════════════════════════════════════════╗
║  ✅ Smart HVAC Database Initialized Successfully!                ║
║                                                                  ║
║  Next steps:                                                     ║
║  1. Install Python dependencies: pip install -r requirements.txt║
║  2. Start the system: python scripts/start_system.py            ║
║  3. Access dashboard: http://localhost:5000                      ║
╚══════════════════════════════════════════════════════════════════╝
        """)
    else:
        print("⚠️ Installation completed with warnings")

if __name__ == "__main__":
    main()