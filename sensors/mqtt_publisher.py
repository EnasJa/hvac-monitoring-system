"""
מפיץ MQTT לשליחת נתוני חיישנים
"""
import json
import time
import threading
from datetime import datetime
from typing import List, Callable, Optional
import paho.mqtt.client as mqtt
from config.system_config import SystemConfig
from sensors.virtual_sensors import SensorReading, SensorManager

class MQTTPublisher:
    """מפיץ MQTT לנתוני חיישנים"""
    
    def __init__(self, broker_host: str = "localhost", broker_port: int = 1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client = mqtt.Client(client_id="hvac_sensor_publisher")
        self.is_connected = False
        self.publish_queue = []
        self.config = SystemConfig()
        
        # הגדרת callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        
        print(f"🔌 מפיץ MQTT נוצר - {broker_host}:{broker_port}")
    
    def _on_connect(self, client, userdata, flags, rc):
        """callback חיבור"""
        if rc == 0:
            self.is_connected = True
            print("✅ התחבר לברוקר MQTT בהצלחה")
        else:
            print(f"❌ כשל בחיבור לברוקר MQTT - קוד {rc}")
    
    def _on_disconnect(self, client, userdata, rc):
        """callback ניתוק"""
        self.is_connected = False
        print("⚠️ נותק מברוקר MQTT")
    
    def _on_publish(self, client, userdata, mid):
        """callback פרסום"""
        pass  # ניתן להוסיף לוגים אם נדרש
    
    def connect(self) -> bool:
        """התחברות לברוקר MQTT"""
        try:
            self.client.connect(self.broker_host, self.broker_port, 60)
            self.client.loop_start()
            
            # המתנה לחיבור
            timeout = 10
            while not self.is_connected and timeout > 0:
                time.sleep(0.5)
                timeout -= 0.5
            
            return self.is_connected
            
        except Exception as e:
            print(f"❌ שגיאה בחיבור ל-MQTT: {e}")
            return False
    
    def disconnect(self):
        """ניתוק מהברוקר"""
        if self.is_connected:
            self.client.loop_stop()
            self.client.disconnect()
    
    def publish_sensor_reading(self, reading: SensorReading) -> bool:
        """פרסום קריאת חיישן יחיד"""
        if not self.is_connected:
            print("⚠️ אין חיבור ל-MQTT")
            return False
        
        try:
            # הכנת הנתונים
            payload = {
                'sensor_id': reading.sensor_id,
                'location': reading.location,
                'timestamp': reading.timestamp.isoformat(),
                'data': {
                    'temperature': reading.temperature,
                    'humidity': reading.humidity,
                    'co2': reading.co2,
                    'air_quality_index': reading.air_quality_index,
                    'occupancy': reading.occupancy
                },
                'system_status': reading.system_status,
                'metadata': {
                    'source': 'virtual_sensor',
                    'version': '1.0.0'
                }
            }
            
            # נושא ספציפי לחיישן
            topic = f"hvac/sensors/{reading.sensor_id}/data"
            
            # פרסום
            result = self.client.publish(topic, json.dumps(payload), qos=1)
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                return True
            else:
                print(f"❌ כשל בפרסום - חיישן {reading.sensor_id}")
                return False
                
        except Exception as e:
            print(f"❌ שגיאה בפרסום חיישן {reading.sensor_id}: {e}")
            return False
    
    def publish_multiple_readings(self, readings: List[SensorReading]) -> int:
        """פרסום מספר קריאות"""
        success_count = 0
        
        for reading in readings:
            if self.publish_sensor_reading(reading):
                success_count += 1
        
        return success_count
    
    def publish_system_status(self, status_data: dict):
        """פרסום סטטוס כללי של המערכת"""
        if not self.is_connected:
            return False
        
        try:
            payload = {
                'timestamp': datetime.now().isoformat(),
                'system_status': status_data,
                'metadata': {
                    'source': 'hvac_system',
                    'type': 'system_status'
                }
            }
            
            topic = "hvac/system/status"
            result = self.client.publish(topic, json.dumps(payload), qos=1)
            
            return result.rc == mqtt.MQTT_ERR_SUCCESS
            
        except Exception as e:
            print(f"❌ שגיאה בפרסום סטטוס מערכת: {e}")
            return False
    
    def publish_alert(self, alert_data: dict):
        """פרסום התראה"""
        if not self.is_connected:
            return False
        
        try:
            payload = {
                'timestamp': datetime.now().isoformat(),
                'alert': alert_data,
                'metadata': {
                    'source': 'hvac_alert_system',
                    'type': 'alert'
                }
            }
            
            topic = "hvac/alerts"
            result = self.client.publish(topic, json.dumps(payload), qos=2)  # QoS גבוה להתראות
            
            return result.rc == mqtt.MQTT_ERR_SUCCESS
            
        except Exception as e:
            print(f"❌ שגיאה בפרסום התראה: {e}")
            return False

class HVACDataStreamer:
    """מזרים נתונים מתמשך למערכת HVAC"""
    
    def __init__(self, mqtt_publisher: MQTTPublisher, sensor_manager: SensorManager):
        self.mqtt_publisher = mqtt_publisher
        self.sensor_manager = sensor_manager
        self.is_streaming = False
        self.stream_thread = None
        self.stream_interval = 5  # שניות
        
        # סטטיסטיקות
        self.total_published = 0
        self.failed_publishes = 0
        self.start_time = None
        
    def start_streaming(self, interval: int = 5):
        """התחלת זרימת נתונים"""
        if self.is_streaming:
            print("⚠️ זרימת נתונים כבר פעילה")
            return
        
        if not self.mqtt_publisher.is_connected:
            print("❌ אין חיבור ל-MQTT - לא ניתן להתחיל זרימה")
            return
        
        self.stream_interval = interval
        self.is_streaming = True
        self.start_time = datetime.now()
        
        # יצירת thread לזרימה
        self.stream_thread = threading.Thread(target=self._streaming_loop)
        self.stream_thread.daemon = True
        self.stream_thread.start()
        
        print(f"🚀 זרימת נתונים התחילה (כל {interval} שניות)")
    
    def stop_streaming(self):
        """עצירת זרימת נתונים"""
        if not self.is_streaming:
            return
        
        self.is_streaming = False
        
        if self.stream_thread:
            self.stream_thread.join()
        
        # הדפסת סטטיסטיקות
        duration = datetime.now() - self.start_time if self.start_time else timedelta(0)
        success_rate = (self.total_published / (self.total_published + self.failed_publishes) * 100) if (self.total_published + self.failed_publishes) > 0 else 0
        
        print(f"""
⏹️ זרימת נתונים נעצרה
📊 סטטיסטיקות:
   ⏱️  משך זמן: {duration}
   ✅ הודעות שנשלחו: {self.total_published}
   ❌ כשלונות: {self.failed_publishes}
   📈 אחוז הצלחה: {success_rate:.1f}%
        """)
    
    def _streaming_loop(self):
        """לולאת זרימת נתונים"""
        while self.is_streaming:
            try:
                # קריאת כל החיישנים
                readings = self.sensor_manager.read_all_sensors()
                
                # פרסום הנתונים
                success_count = self.mqtt_publisher.publish_multiple_readings(readings)
                
                # עדכון סטטיסטיקות
                self.total_published += success_count
                self.failed_publishes += len(readings) - success_count
                
                # פרסום סטטוס מערכת כל 60 שניות
                if self.total_published % 12 == 0:  # כל 12 מחזורים (60 שניות ב-5 שניות interval)
                    system_status = self._get_system_status(readings)
                    self.mqtt_publisher.publish_system_status(system_status)
                
                # המתנה עד המחזור הבא
                time.sleep(self.stream_interval)
                
            except Exception as e:
                print(f"❌ שגיאה בלולאת הזרימה: {e}")
                time.sleep(1)
    
    def _get_system_status(self, readings: List[SensorReading]) -> dict:
        """חישוב סטטוס כללי של המערכת"""
        if not readings:
            return {'status': 'NO_DATA', 'sensors_count': 0}
        
        # ספירת סטטוסים
        status_counts = {}
        total_temp = 0
        total_humidity = 0
        total_co2 = 0
        total_occupancy = 0
        
        for reading in readings:
            status = reading.system_status
            status_counts[status] = status_counts.get(status, 0) + 1
            
            total_temp += reading.temperature
            total_humidity += reading.humidity
            total_co2 += reading.co2
            total_occupancy += reading.occupancy
        
        count = len(readings)
        
        # קביעת סטטוס כללי
        if 'CRITICAL' in status_counts:
            overall_status = 'CRITICAL'
        elif 'MALFUNCTION' in status_counts:
            overall_status = 'MALFUNCTION'
        elif 'WARNING' in status_counts:
            overall_status = 'WARNING'
        else:
            overall_status = 'NORMAL'
        
        return {
            'status': overall_status,
            'sensors_count': count,
            'status_breakdown': status_counts,
            'averages': {
                'temperature': round(total_temp / count, 1),
                'humidity': round(total_humidity / count, 1),
                'co2': int(total_co2 / count),
                'occupancy': int(total_occupancy / count)
            },
            'total_occupancy': total_occupancy,
            'timestamp': datetime.now().isoformat()
        }

# פונקציות עזר
def create_mqtt_system() -> tuple:
    """יצירת מערכת MQTT מלאה"""
    # יצירת רכיבים
    sensor_manager = SensorManager()
    mqtt_publisher = MQTTPublisher()
    data_streamer = HVACDataStreamer(mqtt_publisher, sensor_manager)
    
    return sensor_manager, mqtt_publisher, data_streamer

def test_mqtt_connection() -> bool:
    """בדיקת חיבור MQTT"""
    try:
        publisher = MQTTPublisher()
        return publisher.connect()
    except Exception as e:
        print(f"❌ בדיקת MQTT נכשלה: {e}")
        return False

# בדיקה ישירה
if __name__ == "__main__":
    print("🔧 בדיקת מערכת MQTT...")
    
    # יצירת מערכת
    sensor_manager, mqtt_publisher, data_streamer = create_mqtt_system()
    
    # בדיקת חיבור
    if mqtt_publisher.connect():
        print("✅ חיבור MQTT הצליח")
        
        # בדיקת פרסום חד-פעמי
        readings = sensor_manager.read_all_sensors()
        success_count = mqtt_publisher.publish_multiple_readings(readings)
        print(f"📤 פורסמו {success_count}/{len(readings)} הודעות")
        
        # בדיקת זרימה קצרה
        print("\n🚀 התחלת זרימת נתונים למשך 15 שניות...")
        data_streamer.start_streaming(interval=3)
        time.sleep(15)
        data_streamer.stop_streaming()
        
        # ניתוק
        mqtt_publisher.disconnect()
        print("✅ בדיקה הושלמה בהצלחה!")
        
    else:
        print("❌ לא ניתן להתחבר לברוקר MQTT")
        print("💡 וודא שברוקר MQTT (כמו Mosquitto) רץ על localhost:1883")