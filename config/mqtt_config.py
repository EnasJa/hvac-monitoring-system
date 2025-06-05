"""
MQTT Configuration for HVAC Monitoring System
תצורת MQTT למערכת HVAC
"""
import os
import logging
from typing import Dict, List
import paho.mqtt.client as mqtt
import json
from datetime import datetime

class MQTTConfig:
    """תצורת MQTT"""
    
    # הגדרות חיבור
    BROKER_HOST = os.getenv('MQTT_BROKER', 'localhost')
    BROKER_PORT = int(os.getenv('MQTT_PORT', '1883'))
    KEEPALIVE = int(os.getenv('MQTT_KEEPALIVE', '60'))
    
    # אימות
    USERNAME = os.getenv('MQTT_USERNAME', None)
    PASSWORD = os.getenv('MQTT_PASSWORD', None)
    
    # הגדרות חיבור
    CLEAN_SESSION = True
    QOS = 1  # Quality of Service
    RETAIN = False
    
    # נושאי MQTT
    TOPICS = {
        'sensor_data': 'hvac/sensors/{sensor_id}/data',
        'sensor_status': 'hvac/sensors/{sensor_id}/status',
        'alerts': 'hvac/alerts',
        'system_status': 'hvac/system/status',
        'commands': 'hvac/commands/{sensor_id}',
        'maintenance': 'hvac/maintenance',
        'energy': 'hvac/energy',
        'analytics': 'hvac/analytics'
    }
    
    # נושאי הרשמה (wildcards)
    SUBSCRIBE_TOPICS = [
        ('hvac/sensors/+/data', 1),
        ('hvac/sensors/+/status', 1),
        ('hvac/commands/+', 1),
        ('hvac/system/+', 1)
    ]
    
    # הגדרות חיבור מחדש
    RECONNECT_DELAY_MIN = 1
    RECONNECT_DELAY_MAX = 60
    RECONNECT_DELAY_SET = [1, 2, 4, 8, 16, 32, 60]

class MQTTClient:
    """לקוח MQTT למערכת HVAC"""
    
    def __init__(self, client_id: str = None):
        self.client_id = client_id or f"hvac_monitor_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.client = mqtt.Client(self.client_id)
        self.config = MQTTConfig()
        self.logger = logging.getLogger(__name__)
        
        # רשימת callbacks
        self.message_callbacks = {}
        self.connection_callbacks = []
        self.disconnect_callbacks = []
        
        # הגדרת callbacks בסיסיים
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_publish = self._on_publish
        self.client.on_subscribe = self._on_subscribe
        
        # הגדרת אימות אם נדרש
        if self.config.USERNAME and self.config.PASSWORD:
            self.client.username_pw_set(self.config.USERNAME, self.config.PASSWORD)
        
        self.is_connected = False
    
    def _on_connect(self, client, userdata, flags, rc):
        """callback לחיבור"""
        if rc == 0:
            self.is_connected = True
            self.logger.info(f"✅ התחבר ל-MQTT broker: {self.config.BROKER_HOST}:{self.config.BROKER_PORT}")
            
            # הרשמה לנושאים
            for topic, qos in self.config.SUBSCRIBE_TOPICS:
                client.subscribe(topic, qos)
                self.logger.info(f"🔔 נרשם לנושא: {topic}")
            
            # קריאה ל-callbacks של חיבור
            for callback in self.connection_callbacks:
                try:
                    callback()
                except Exception as e:
                    self.logger.error(f"❌ שגיאה ב-callback חיבור: {e}")
        else:
            self.is_connected = False
            self.logger.error(f"❌ כישלון בחיבור ל-MQTT broker: {rc}")
    
    def _on_disconnect(self, client, userdata, rc):
        """callback לניתוק"""
        self.is_connected = False
        if rc != 0:
            self.logger.warning(f"⚠️ ניתוק לא צפוי מ-MQTT broker: {rc}")
        else:
            self.logger.info("🔌 ניתוק מ-MQTT broker")
        
        # קריאה ל-callbacks של ניתוק
        for callback in self.disconnect_callbacks:
            try:
                callback()
            except Exception as e:
                self.logger.error(f"❌ שגיאה ב-callback ניתוק: {e}")
    
    def _on_message(self, client, userdata, msg):
        """callback להודעות"""
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            
            self.logger.debug(f"📨 הודעה התקבלה - נושא: {topic}, תוכן: {payload[:100]}...")
            
            # חיפוש callback מתאים
            callback_found = False
            for pattern, callback in self.message_callbacks.items():
                if self._topic_matches(topic, pattern):
                    try:
                        # נסיון לפרש כ-JSON
                        try:
                            data = json.loads(payload)
                        except json.JSONDecodeError:
                            data = payload
                        
                        callback(topic, data)
                        callback_found = True
                        break
                    except Exception as e:
                        self.logger.error(f"❌ שגיאה בעיבוד הודעה: {e}")
            
            if not callback_found:
                self.logger.warning(f"⚠️ לא נמצא callback לנושא: {topic}")
                
        except Exception as e:
            self.logger.error(f"❌ שגיאה כללית בעיבוד הודעה: {e}")
    
    def _on_publish(self, client, userdata, mid):
        """callback לפרסום"""
        self.logger.debug(f"📤 הודעה פורסמה: {mid}")
    
    def _on_subscribe(self, client, userdata, mid, granted_qos):
        """callback להרשמה"""
        self.logger.debug(f"✅ הרשמה הושלמה: {mid}, QoS: {granted_qos}")
    
    def _topic_matches(self, topic: str, pattern: str) -> bool:
        """בדיקה אם נושא תואם לתבנית"""
        topic_parts = topic.split('/')
        pattern_parts = pattern.split('/')
        
        if len(topic_parts) != len(pattern_parts):
            return False
        
        for topic_part, pattern_part in zip(topic_parts, pattern_parts):
            if pattern_part == '+':
                continue
            elif pattern_part == '#':
                return True
            elif topic_part != pattern_part:
                return False
        
        return True
    
    def connect(self) -> bool:
        """התחברות ל-MQTT broker"""
        try:
            self.logger.info(f"🔌 מתחבר ל-MQTT broker: {self.config.BROKER_HOST}:{self.config.BROKER_PORT}")
            self.client.connect(
                self.config.BROKER_HOST,
                self.config.BROKER_PORT,
                self.config.KEEPALIVE
            )
            self.client.loop_start()
            return True
        except Exception as e:
            self.logger.error(f"❌ שגיאה בחיבור ל-MQTT: {e}")
            return False
    
    def disconnect(self):
        """ניתוק מ-MQTT broker"""
        try:
            self.client.loop_stop()
            self.client.disconnect()
            self.logger.info("🔌 ניתוק מ-MQTT broker")
        except Exception as e:
            self.logger.error(f"❌ שגיאה בניתוק מ-MQTT: {e}")
    
    def publish(self, topic: str, payload: any, qos: int = None, retain: bool = None) -> bool:
        """פרסום הודעה"""
        try:
            if not self.is_connected:
                self.logger.warning("⚠️ לא מחובר ל-MQTT broker")
                return False
            
            # המרה ל-JSON אם נדרש
            if isinstance(payload, (dict, list)):
                payload = json.dumps(payload, ensure_ascii=False)
            
            qos = qos or self.config.QOS
            retain = retain or self.config.RETAIN
            
            result = self.client.publish(topic, payload, qos, retain)
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                self.logger.debug(f"📤 הודעה פורסמה לנושא: {topic}")
                return True
            else:
                self.logger.error(f"❌ כישלון בפרסום הודעה: {result.rc}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ שגיאה בפרסום הודעה: {e}")
            return False
    
    def subscribe(self, topic: str, callback, qos: int = None):
        """הרשמה לנושא עם callback"""
        qos = qos or self.config.QOS
        
        # רישום callback
        self.message_callbacks[topic] = callback
        
        # הרשמה לנושא אם מחובר
        if self.is_connected:
            self.client.subscribe(topic, qos)
            self.logger.info(f"🔔 נרשם לנושא: {topic}")
    
    def unsubscribe(self, topic: str):
        """ביטול הרשמה לנושא"""
        if topic in self.message_callbacks:
            del self.message_callbacks[topic]
        
        if self.is_connected:
            self.client.unsubscribe(topic)
            self.logger.info(f"🔕 בוטלה הרשמה לנושא: {topic}")
    
    def add_connection_callback(self, callback):
        """הוספת callback לחיבור"""
        self.connection_callbacks.append(callback)
    
    def add_disconnect_callback(self, callback):
        """הוספת callback לניתוק"""
        self.disconnect_callbacks.append(callback)
    
    def publish_sensor_data(self, sensor_id: str, data: dict) -> bool:
        """פרסום נתוני חיישן"""
        topic = self.config.TOPICS['sensor_data'].format(sensor_id=sensor_id)
        
        # הוספת מטא-דטה
        message = {
            'sensor_id': sensor_id,
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        
        return self.publish(topic, message)
    
    def publish_alert(self, alert_data: dict) -> bool:
        """פרסום התראה"""
        topic = self.config.TOPICS['alerts']
        
        # הוספת מטא-דטה
        message = {
            'timestamp': datetime.now().isoformat(),
            'alert': alert_data
        }
        
        return self.publish(topic, message)
    
    def publish_system_status(self, status_data: dict) -> bool:
        """פרסום סטטוס מערכת"""
        topic = self.config.TOPICS['system_status']
        
        # הוספת מטא-דטה
        message = {
            'timestamp': datetime.now().isoformat(),
            'system_status': status_data
        }
        
        return self.publish(topic, message)
    
    def send_command(self, sensor_id: str, command: dict) -> bool:
        """שליחת פקודה לחיישן"""
        topic = self.config.TOPICS['commands'].format(sensor_id=sensor_id)
        
        # הוספת מטא-דטה
        message = {
            'timestamp': datetime.now().isoformat(),
            'command': command
        }
        
        return self.publish(topic, message)

class MQTTMessageHandler:
    """מטפל בהודעות MQTT"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.sensor_data_handlers = []
        self.alert_handlers = []
        self.status_handlers = []
        self.command_handlers = []
    
    def add_sensor_data_handler(self, handler):
        """הוספת מטפל לנתוני חיישנים"""
        self.sensor_data_handlers.append(handler)
    
    def add_alert_handler(self, handler):
        """הוספת מטפל להתראות"""
        self.alert_handlers.append(handler)
    
    def add_status_handler(self, handler):
        """הוספת מטפל לסטטוס"""
        self.status_handlers.append(handler)
    
    def add_command_handler(self, handler):
        """הוספת מטפל לפקודות"""
        self.command_handlers.append(handler)
    
    def handle_sensor_data(self, topic: str, data: dict):
        """טיפול בנתוני חיישן"""
        try:
            # חילוץ sensor_id מהנושא
            sensor_id = topic.split('/')[2]
            
            self.logger.debug(f"📊 מעבד נתוני חיישן: {sensor_id}")
            
            # קריאה לכל המטפלים
            for handler in self.sensor_data_handlers:
                try:
                    handler(sensor_id, data)
                except Exception as e:
                    self.logger.error(f"❌ שגיאה במטפל נתוני חיישן: {e}")
                    
        except Exception as e:
            self.logger.error(f"❌ שגיאה בטיפול בנתוני חיישן: {e}")
    
    def handle_alert(self, topic: str, data: dict):
        """טיפול בהתראה"""
        try:
            self.logger.info(f"🚨 מעבד התראה: {data}")
            
            # קריאה לכל המטפלים
            for handler in self.alert_handlers:
                try:
                    handler(data)
                except Exception as e:
                    self.logger.error(f"❌ שגיאה במטפל התראות: {e}")
                    
        except Exception as e:
            self.logger.error(f"❌ שגיאה בטיפול בהתראה: {e}")
    
    def handle_system_status(self, topic: str, data: dict):
        """טיפול בסטטוס מערכת"""
        try:
            self.logger.debug(f"🔧 מעבד סטטוס מערכת: {data}")
            
            # קריאה לכל המטפלים
            for handler in self.status_handlers:
                try:
                    handler(data)
                except Exception as e:
                    self.logger.error(f"❌ שגיאה במטפל סטטוס: {e}")
                    
        except Exception as e:
            self.logger.error(f"❌ שגיאה בטיפול בסטטוס מערכת: {e}")
    
    def handle_command(self, topic: str, data: dict):
        """טיפול בפקודה"""
        try:
            # חילוץ sensor_id מהנושא
            sensor_id = topic.split('/')[2]
            
            self.logger.info(f"📡 מעבד פקודה לחיישן {sensor_id}: {data}")
            
            # קריאה לכל המטפלים
            for handler in self.command_handlers:
                try:
                    handler(sensor_id, data)
                except Exception as e:
                    self.logger.error(f"❌ שגיאה במטפל פקודות: {e}")
                    
        except Exception as e:
            self.logger.error(f"❌ שגיאה בטיפול בפקודה: {e}")

# פונקציות עזר
def create_mqtt_client(client_id: str = None) -> MQTTClient:
    """יצירת לקוח MQTT"""
    return MQTTClient(client_id)

def setup_mqtt_handlers(client: MQTTClient, handler: MQTTMessageHandler):
    """הגדרת מטפלי הודעות"""
    # הרשמה לנושאי חיישנים
    client.subscribe('hvac/sensors/+/data', handler.handle_sensor_data)
    
    # הרשמה להתראות
    client.subscribe('hvac/alerts', handler.handle_alert)
    
    # הרשמה לסטטוס מערכת
    client.subscribe('hvac/system/+', handler.handle_system_status)
    
    # הרשמה לפקודות
    client.subscribe('hvac/commands/+', handler.handle_command)

# בדיקה ישירה
if __name__ == "__main__":
    print("🔧 בדיקת MQTT Configuration...")
    
    # יצירת לקוח
    client = create_mqtt_client("test_client")
    
    # יצירת מטפל הודעות
    handler = MQTTMessageHandler()
    
    # הוספת מטפלי בדיקה
    def test_sensor_handler(sensor_id, data):
        print(f"📊 נתוני חיישן: {sensor_id} = {data}")
    
    def test_alert_handler(alert_data):
        print(f"🚨 התראה: {alert_data}")
    
    handler.add_sensor_data_handler(test_sensor_handler)
    handler.add_alert_handler(test_alert_handler)
    
    # הגדרת מטפלים
    setup_mqtt_handlers(client, handler)
    
    # התחברות
    if client.connect():
        print("✅ התחבר בהצלחה ל-MQTT")
        
        # פרסום בדיקה
        test_data = {
            'temperature': 23.5,
            'humidity': 45.2,
            'co2': 450
        }
        
        client.publish_sensor_data('test_sensor', test_data)
        
        # המתנה קצרה ואז ניתוק
        import time
        time.sleep(2)
        
        client.disconnect()
    else:
        print("❌ כישלון בחיבור ל-MQTT")
    
    print("✅ בדיקה הושלמה!")