"""
מודלי בסיס נתונים למערכת HVAC
"""
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, Boolean, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.sqlite import JSON
import json

Base = declarative_base()

class Sensor(Base):
    """מודל חיישן"""
    __tablename__ = 'sensors'
    
    id = Column(Integer, primary_key=True)
    sensor_id = Column(String(50), unique=True, nullable=False)
    location = Column(String(100), nullable=False)
    description = Column(Text)
    coordinates_x = Column(Float)
    coordinates_y = Column(Float)
    capacity = Column(Integer, default=0)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # קשרים
    readings = relationship("SensorReading", back_populates="sensor")
    alerts = relationship("Alert", back_populates="sensor")
    
    def __repr__(self):
        return f"<Sensor(sensor_id='{self.sensor_id}', location='{self.location}')>"

class SensorReading(Base):
    """מודל קריאת חיישן"""
    __tablename__ = 'sensor_readings'
    
    id = Column(Integer, primary_key=True)
    sensor_id = Column(String(50), ForeignKey('sensors.sensor_id'), nullable=False)
    timestamp = Column(DateTime, nullable=False)
    
    # נתוני חיישן
    temperature = Column(Float, nullable=False)
    humidity = Column(Float, nullable=False)
    co2 = Column(Integer, nullable=False)
    air_quality_index = Column(Float)
    occupancy = Column(Integer, default=0)
    
    # נתונים מעובדים
    filtered_temperature = Column(Float)
    filtered_humidity = Column(Float)
    filtered_co2 = Column(Float)
    anomaly_score = Column(Float, default=0.0)
    quality_score = Column(Float, default=1.0)
    
    # סטטוס
    system_status = Column(String(20), default='NORMAL')
    
    # מטא-דטה
    created_at = Column(DateTime, default=datetime.now)
    
    # קשרים
    sensor = relationship("Sensor", back_populates="readings")
    
    def __repr__(self):
        return f"<SensorReading(sensor_id='{self.sensor_id}', temp={self.temperature}, time='{self.timestamp}')>"
    
    def to_dict(self):
        """המרה למילון"""
        return {
            'id': self.id,
            'sensor_id': self.sensor_id,
            'timestamp': self.timestamp.isoformat(),
            'temperature': self.temperature,
            'humidity': self.humidity,
            'co2': self.co2,
            'air_quality_index': self.air_quality_index,
            'occupancy': self.occupancy,
            'filtered_temperature': self.filtered_temperature,
            'filtered_humidity': self.filtered_humidity,
            'filtered_co2': self.filtered_co2,
            'anomaly_score': self.anomaly_score,
            'quality_score': self.quality_score,
            'system_status': self.system_status
        }

class Alert(Base):
    """מודל התראה"""
    __tablename__ = 'alerts'
    
    id = Column(Integer, primary_key=True)
    alert_id = Column(String(100), unique=True, nullable=False)
    sensor_id = Column(String(50), ForeignKey('sensors.sensor_id'), nullable=False)
    
    # פרטי התראה
    alert_type = Column(String(50), nullable=False)
    severity = Column(String(20), nullable=False)  # LOW, MEDIUM, HIGH, CRITICAL
    message = Column(Text, nullable=False)
    
    # ערכים שגרמו להתראה
    trigger_values = Column(Text)  # JSON string
    threshold_violated = Column(String(50))
    recommended_action = Column(Text)
    
    # זמנים
    timestamp = Column(DateTime, nullable=False)
    acknowledged_at = Column(DateTime)
    resolved_at = Column(DateTime)
    
    # סטטוס
    is_active = Column(Boolean, default=True)
    is_acknowledged = Column(Boolean, default=False)
    is_resolved = Column(Boolean, default=False)
    
    # מטא-דטה
    created_at = Column(DateTime, default=datetime.now)
    
    # קשרים
    sensor = relationship("Sensor", back_populates="alerts")
    
    def __repr__(self):
        return f"<Alert(alert_id='{self.alert_id}', severity='{self.severity}', type='{self.alert_type}')>"
    
    def to_dict(self):
        """המרה למילון"""
    def to_dict(self):
        """המרה למילון"""
        return {
            'id': self.id,
            'alert_id': self.alert_id,
            'sensor_id': self.sensor_id,
            'alert_type': self.alert_type,
            'severity': self.severity,
            'message': self.message,
            'trigger_values': json.loads(self.trigger_values) if self.trigger_values else {},
            'threshold_violated': self.threshold_violated,
            'recommended_action': self.recommended_action,
            'timestamp': self.timestamp.isoformat(),
            'acknowledged_at': self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            'resolved_at': self.resolved_at.isoformat() if self.resolved_at else None,
            'is_active': self.is_active,
            'is_acknowledged': self.is_acknowledged,
            'is_resolved': self.is_resolved
        }

class SystemStatus(Base):
    """מודל סטטוס מערכת"""
    __tablename__ = 'system_status'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    
    # סטטוס כללי
    overall_status = Column(String(20), default='NORMAL')
    active_sensors_count = Column(Integer, default=0)
    total_alerts_count = Column(Integer, default=0)
    critical_alerts_count = Column(Integer, default=0)
    
    # ממוצעים כלליים
    avg_temperature = Column(Float)
    avg_humidity = Column(Float)
    avg_co2 = Column(Float)
    total_occupancy = Column(Integer, default=0)
    
    # ביצועי מערכת
    data_quality_avg = Column(Float, default=1.0)
    anomaly_rate = Column(Float, default=0.0)
    
    # מטא-דטה
    created_at = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<SystemStatus(status='{self.overall_status}', time='{self.timestamp}')>"
    
    def to_dict(self):
        """המרה למילון"""
        return {
            'id': self.id,
            'timestamp': self.timestamp.isoformat(),
            'overall_status': self.overall_status,
            'active_sensors_count': self.active_sensors_count,
            'total_alerts_count': self.total_alerts_count,
            'critical_alerts_count': self.critical_alerts_count,
            'avg_temperature': self.avg_temperature,
            'avg_humidity': self.avg_humidity,
            'avg_co2': self.avg_co2,
            'total_occupancy': self.total_occupancy,
            'data_quality_avg': self.data_quality_avg,
            'anomaly_rate': self.anomaly_rate
        }

class EnergyLog(Base):
    """מודל לוג צריכת אנרגיה"""
    __tablename__ = 'energy_logs'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    
    # צריכת אנרגיה (מדומה)
    cooling_energy = Column(Float, default=0.0)  # kWh
    heating_energy = Column(Float, default=0.0)  # kWh
    ventilation_energy = Column(Float, default=0.0)  # kWh
    total_energy = Column(Float, default=0.0)  # kWh
    
    # יעילות
    efficiency_score = Column(Float, default=1.0)
    cost_estimate = Column(Float, default=0.0)  # שקלים
    
    # מטא-דטה
    created_at = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<EnergyLog(total={self.total_energy}kWh, time='{self.timestamp}')>"

class MaintenanceLog(Base):
    """מודל לוג תחזוקה"""
    __tablename__ = 'maintenance_logs'
    
    id = Column(Integer, primary_key=True)
    sensor_id = Column(String(50), ForeignKey('sensors.sensor_id'))
    
    # פרטי תחזוקה
    maintenance_type = Column(String(50), nullable=False)  # PREVENTIVE, CORRECTIVE, EMERGENCY
    description = Column(Text, nullable=False)
    technician_name = Column(String(100))
    
    # זמנים
    scheduled_date = Column(DateTime)
    completed_date = Column(DateTime)
    
    # סטטוס
    status = Column(String(20), default='PENDING')  # PENDING, IN_PROGRESS, COMPLETED, CANCELLED
    priority = Column(String(20), default='NORMAL')  # LOW, NORMAL, HIGH, URGENT
    
    # עלויות
    estimated_cost = Column(Float, default=0.0)
    actual_cost = Column(Float, default=0.0)
    
    # מטא-דטה
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # קשרים
    sensor = relationship("Sensor")
    
    def __repr__(self):
        return f"<MaintenanceLog(type='{self.maintenance_type}', sensor='{self.sensor_id}')>"

class DatabaseManager:
    """מנהל בסיס הנתונים"""
    
    def __init__(self, database_url: str = "sqlite:///database/hvac_data.db"):
        self.database_url = database_url
        self.engine = create_engine(database_url, echo=False)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        
        # יצירת טבלאות
        self.create_tables()
    
    def create_tables(self):
        """יצירת כל הטבלאות"""
        Base.metadata.create_all(bind=self.engine)
        print("✅ טבלאות בסיס הנתונים נוצרו בהצלחה")
    
    def get_session(self):
        """קבלת session חדש"""
        return self.SessionLocal()
    
    def initialize_sensors(self):
        """אתחול חיישנים בבסיס הנתונים"""
        from config.system_config import SystemConfig
        
        config = SystemConfig()
        session = self.get_session()
        
        try:
            # בדיקה אם כבר קיימים חיישנים
            existing_count = session.query(Sensor).count()
            if existing_count > 0:
                print(f"ℹ️ נמצאו {existing_count} חיישנים קיימים")
                return
            
            # יצירת חיישנים חדשים
            for location_id, location_config in config.SENSOR_LOCATIONS.items():
                sensor = Sensor(
                    sensor_id=f"hvac_{location_id}",
                    location=location_config['name'],
                    description=location_config['description'],
                    coordinates_x=location_config['coordinates']['x'],
                    coordinates_y=location_config['coordinates']['y'],
                    capacity=location_config['capacity']
                )
                session.add(sensor)
            
            session.commit()
            print(f"✅ אותחלו {len(config.SENSOR_LOCATIONS)} חיישנים בבסיס הנתונים")
            
        except Exception as e:
            session.rollback()
            print(f"❌ שגיאה באתחול חיישנים: {e}")
        finally:
            session.close()
    
    def save_sensor_reading(self, reading_data: dict):
        """שמירת קריאת חיישן"""
        session = self.get_session()
        
        try:
            reading = SensorReading(
                sensor_id=reading_data['sensor_id'],
                timestamp=datetime.fromisoformat(reading_data['timestamp']) if isinstance(reading_data['timestamp'], str) else reading_data['timestamp'],
                temperature=reading_data['temperature'],
                humidity=reading_data['humidity'],
                co2=reading_data['co2'],
                air_quality_index=reading_data.get('air_quality_index'),
                occupancy=reading_data.get('occupancy', 0),
                filtered_temperature=reading_data.get('filtered_temperature'),
                filtered_humidity=reading_data.get('filtered_humidity'),
                filtered_co2=reading_data.get('filtered_co2'),
                anomaly_score=reading_data.get('anomaly_score', 0.0),
                quality_score=reading_data.get('quality_score', 1.0),
                system_status=reading_data.get('system_status', 'NORMAL')
            )
            
            session.add(reading)
            session.commit()
            return reading.id
            
        except Exception as e:
            session.rollback()
            print(f"❌ שגיאה בשמירת קריאה: {e}")
            return None
        finally:
            session.close()
    
    def save_alert(self, alert_data: dict):
        """שמירת התראה"""
        session = self.get_session()
        
        try:
            alert = Alert(
                alert_id=alert_data['alert_id'],
                sensor_id=alert_data['sensor_id'],
                alert_type=alert_data['alert_type'],
                severity=alert_data['severity'],
                message=alert_data['message'],
                trigger_values=json.dumps(alert_data.get('values', {})),
                threshold_violated=alert_data.get('threshold_violated'),
                recommended_action=alert_data.get('recommended_action'),
                timestamp=datetime.fromisoformat(alert_data['timestamp']) if isinstance(alert_data['timestamp'], str) else alert_data['timestamp']
            )
            
            session.add(alert)
            session.commit()
            return alert.id
            
        except Exception as e:
            session.rollback()
            print(f"❌ שגיאה בשמירת התראה: {e}")
            return None
        finally:
            session.close()
    
    def get_latest_readings(self, limit: int = 100):
        """קבלת קריאות אחרונות"""
        session = self.get_session()
        
        try:
            readings = session.query(SensorReading)\
                .order_by(SensorReading.timestamp.desc())\
                .limit(limit)\
                .all()
            
            return [reading.to_dict() for reading in readings]
            
        except Exception as e:
            print(f"❌ שגיאה בקבלת קריאות: {e}")
            return []
        finally:
            session.close()
    
    def get_sensor_readings(self, sensor_id: str, hours: int = 24):
        """קבלת קריאות לחיישן ספציפי"""
        session = self.get_session()
        
        try:
            since = datetime.now() - timedelta(hours=hours)
            
            readings = session.query(SensorReading)\
                .filter(SensorReading.sensor_id == sensor_id)\
                .filter(SensorReading.timestamp >= since)\
                .order_by(SensorReading.timestamp.desc())\
                .all()
            
            return [reading.to_dict() for reading in readings]
            
        except Exception as e:
            print(f"❌ שגיאה בקבלת קריאות חיישן: {e}")
            return []
        finally:
            session.close()
    
    def get_active_alerts(self):
        """קבלת התראות פעילות"""
        session = self.get_session()
        
        try:
            alerts = session.query(Alert)\
                .filter(Alert.is_active == True)\
                .order_by(Alert.timestamp.desc())\
                .all()
            
            return [alert.to_dict() for alert in alerts]
            
        except Exception as e:
            print(f"❌ שגיאה בקבלת התראות: {e}")
            return []
        finally:
            session.close()
    
    def acknowledge_alert(self, alert_id: str):
        """אישור התראה"""
        session = self.get_session()
        
        try:
            alert = session.query(Alert).filter(Alert.alert_id == alert_id).first()
            if alert:
                alert.is_acknowledged = True
                alert.acknowledged_at = datetime.now()
                session.commit()
                return True
            return False
            
        except Exception as e:
            session.rollback()
            print(f"❌ שגיאה באישור התראה: {e}")
            return False
        finally:
            session.close()
    
    def resolve_alert(self, alert_id: str):
        """פתרון התראה"""
        session = self.get_session()
        
        try:
            alert = session.query(Alert).filter(Alert.alert_id == alert_id).first()
            if alert:
                alert.is_resolved = True
                alert.is_active = False
                alert.resolved_at = datetime.now()
                session.commit()
                return True
            return False
            
        except Exception as e:
            session.rollback()
            print(f"❌ שגיאה בפתרון התראה: {e}")
            return False
        finally:
            session.close()
    
    def get_system_statistics(self):
        """קבלת סטטיסטיקות מערכת"""
        session = self.get_session()
        
        try:
            # ספירת חיישנים פעילים
            active_sensors = session.query(Sensor).filter(Sensor.is_active == True).count()
            
            # ספירת התראות פעילות
            active_alerts = session.query(Alert).filter(Alert.is_active == True).count()
            
            # קריאות אחרונות (24 שעות)
            since = datetime.now() - timedelta(hours=24)
            recent_readings = session.query(SensorReading)\
                .filter(SensorReading.timestamp >= since)\
                .count()
            
            # ממוצעים אחרונים
            latest_readings = session.query(SensorReading)\
                .filter(SensorReading.timestamp >= since)\
                .all()
            
            if latest_readings:
                avg_temp = sum(r.temperature for r in latest_readings) / len(latest_readings)
                avg_humidity = sum(r.humidity for r in latest_readings) / len(latest_readings)
                avg_co2 = sum(r.co2 for r in latest_readings) / len(latest_readings)
                avg_quality = sum(r.quality_score for r in latest_readings) / len(latest_readings)
            else:
                avg_temp = avg_humidity = avg_co2 = avg_quality = 0
            
            return {
                'active_sensors': active_sensors,
                'active_alerts': active_alerts,
                'recent_readings_24h': recent_readings,
                'averages_24h': {
                    'temperature': round(avg_temp, 1),
                    'humidity': round(avg_humidity, 1),
                    'co2': int(avg_co2),
                    'quality_score': round(avg_quality, 2)
                }
            }
            
        except Exception as e:
            print(f"❌ שגיאה בקבלת סטטיסטיקות: {e}")
            return {}
        finally:
            session.close()
    
    def cleanup_old_data(self, days_to_keep: int = 365):
        """ניקוי נתונים ישנים"""
        session = self.get_session()
        
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # מחיקת קריאות ישנות
            deleted_readings = session.query(SensorReading)\
                .filter(SensorReading.timestamp < cutoff_date)\
                .delete()
            
            # מחיקת התראות ישנות שנפתרו
            deleted_alerts = session.query(Alert)\
                .filter(Alert.timestamp < cutoff_date)\
                .filter(Alert.is_resolved == True)\
                .delete()
            
            session.commit()
            
            print(f"🧹 נוקו {deleted_readings} קריאות ו-{deleted_alerts} התראות ישנות")
            return deleted_readings + deleted_alerts
            
        except Exception as e:
            session.rollback()
            print(f"❌ שגיאה בניקוי נתונים: {e}")
            return 0
        finally:
            session.close()

# בדיקה ישירה
if __name__ == "__main__":
    print("🔧 בדיקת בסיס נתונים...")
    
    # יצירת מנהל בסיס נתונים
    db_manager = DatabaseManager("sqlite:///test_hvac.db")
    
    # אתחול חיישנים
    db_manager.initialize_sensors()
    
    # בדיקת שמירת נתונים
    test_reading = {
        'sensor_id': 'hvac_office_a1',
        'timestamp': datetime.now(),
        'temperature': 23.5,
        'humidity': 45.2,
        'co2': 450,
        'air_quality_index': 85.5,
        'occupancy': 3,
        'system_status': 'NORMAL'
    }
    
    reading_id = db_manager.save_sensor_reading(test_reading)
    print(f"✅ נשמרה קריאה עם ID: {reading_id}")
    
    # בדיקת שמירת התראה
    test_alert = {
        'alert_id': 'test_alert_001',
        'sensor_id': 'hvac_office_a1',
        'alert_type': 'TEMPERATURE_HIGH',
        'severity': 'MEDIUM',
        'message': 'טמפרטורה גבוהה מהרגיל',
        'values': {'temperature': 28.5},
        'timestamp': datetime.now()
    }
    
    alert_id = db_manager.save_alert(test_alert)
    print(f"🚨 נשמרה התראה עם ID: {alert_id}")
    
    # קבלת סטטיסטיקות
    stats = db_manager.get_system_statistics()
    print(f"📊 סטטיסטיקות: {stats}")
    
    print("✅ בדיקה הושלמה בהצלחה!")