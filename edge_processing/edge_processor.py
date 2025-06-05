"""
מעבד Edge חכם למערכת HVAC
מבצע עיבוד ראשוני, זיהוי חריגות וסינון נתונים
"""
import json
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import deque
import statistics
import numpy as np
from dataclasses import dataclass, asdict
import paho.mqtt.client as mqtt

from config.system_config import SystemConfig
from sensors.virtual_sensors import SensorReading

@dataclass
class ProcessedReading:
    """קריאה מעובדת"""
    original_reading: SensorReading
    processed_timestamp: datetime
    anomaly_score: float
    quality_score: float
    filtered_values: Dict[str, float]
    trend_indicators: Dict[str, str]
    recommendations: List[str]

@dataclass
class Alert:
    """התראת מערכת"""
    alert_id: str
    sensor_id: str
    location: str
    alert_type: str
    severity: str
    message: str
    timestamp: datetime
    values: Dict[str, float]
    threshold_violated: str
    recommended_action: str

class DataBuffer:
    """מאגר נתונים עם חלון זמן"""
    
    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self.readings = deque(maxlen=max_size)
        self.timestamps = deque(maxlen=max_size)
    
    def add_reading(self, reading: SensorReading):
        """הוספת קריאה למאגר"""
        self.readings.append(reading)
        self.timestamps.append(reading.timestamp)
    
    def get_recent_readings(self, minutes: int = 30) -> List[SensorReading]:
        """קבלת קריאות אחרונות לפי זמן"""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        recent = []
        
        for reading, timestamp in zip(self.readings, self.timestamps):
            if timestamp >= cutoff_time:
                recent.append(reading)
        
        return recent
    
    def get_values_for_parameter(self, parameter: str) -> List[float]:
        """קבלת ערכים לפרמטר ספציפי"""
        values = []
        for reading in self.readings:
            if hasattr(reading, parameter):
                values.append(getattr(reading, parameter))
        return values
    
    def calculate_statistics(self, parameter: str) -> Dict[str, float]:
        """חישוב סטטיסטיקות לפרמטר"""
        values = self.get_values_for_parameter(parameter)
        
        if not values:
            return {}
        
        return {
            'mean': statistics.mean(values),
            'median': statistics.median(values),
            'std': statistics.stdev(values) if len(values) > 1 else 0,
            'min': min(values),
            'max': max(values),
            'count': len(values)
        }

class AnomalyDetector:
    """גלאי חריגות סטטיסטי"""
    
    def __init__(self, threshold_multiplier: float = 2.5):
        self.threshold_multiplier = threshold_multiplier
        self.parameter_stats = {}
    
    def detect_anomaly(self, reading: SensorReading, historical_data: DataBuffer) -> Tuple[float, Dict[str, bool]]:
        """זיהוי חריגות בקריאה"""
        anomaly_score = 0.0
        anomalies = {}
        
        parameters = ['temperature', 'humidity', 'co2']
        
        for param in parameters:
            current_value = getattr(reading, param)
            stats = historical_data.calculate_statistics(param)
            
            if stats and stats['count'] >= 10:  # מינימום נתונים
                mean = stats['mean']
                std = stats['std']
                
                if std > 0:
                    # חישוב Z-score
                    z_score = abs((current_value - mean) / std)
                    
                    # זיהוי חריגה
                    is_anomaly = z_score > self.threshold_multiplier
                    anomalies[param] = is_anomaly
                    
                    # תרומה לציון החריגה הכללי
                    if is_anomaly:
                        anomaly_score += min(z_score / self.threshold_multiplier, 3.0)
                else:
                    anomalies[param] = False
            else:
                anomalies[param] = False
        
        # נרמול הציון (0-1)
        anomaly_score = min(anomaly_score / len(parameters), 1.0)
        
        return anomaly_score, anomalies
    
    def detect_trend_anomaly(self, historical_data: DataBuffer, parameter: str) -> str:
        """זיהוי חריגות במגמה"""
        recent_values = historical_data.get_values_for_parameter(parameter)
        
        if len(recent_values) < 5:
            return "insufficient_data"
        
        # חישוב מגמה
        x = list(range(len(recent_values)))
        slope = np.polyfit(x, recent_values, 1)[0]
        
        # סיווג מגמה
        if abs(slope) < 0.1:
            return "stable"
        elif slope > 0.5:
            return "rising_fast"
        elif slope > 0.1:
            return "rising"
        elif slope < -0.5:
            return "falling_fast"
        elif slope < -0.1:
            return "falling"
        else:
            return "stable"

class DataFilter:
    """מסנן נתונים לניקוי רעש"""
    
    def __init__(self, filter_strength: float = 0.1):
        self.filter_strength = filter_strength
        self.last_filtered_values = {}
    
    def apply_exponential_smoothing(self, sensor_id: str, current_value: float, parameter: str) -> float:
        """החלקה אקספוננציאלית"""
        key = f"{sensor_id}_{parameter}"
        
        if key not in self.last_filtered_values:
            self.last_filtered_values[key] = current_value
            return current_value
        
        # החלקה אקספוננציאלית
        alpha = self.filter_strength
        filtered_value = alpha * current_value + (1 - alpha) * self.last_filtered_values[key]
        
        self.last_filtered_values[key] = filtered_value
        return filtered_value
    
    def filter_reading(self, reading: SensorReading) -> Dict[str, float]:
        """סינון קריאת חיישן"""
        filtered = {}
        
        parameters = ['temperature', 'humidity', 'co2', 'air_quality_index']
        
        for param in parameters:
            current_value = float(getattr(reading, param))
            filtered_value = self.apply_exponential_smoothing(
                reading.sensor_id, current_value, param
            )
            filtered[param] = round(filtered_value, 2)
        
        return filtered

class QualityAssessment:
    """הערכת איכות נתונים"""
    
    def __init__(self):
        self.config = SystemConfig()
    
    def assess_quality(self, reading: SensorReading, historical_data: DataBuffer) -> float:
        """הערכת איכות קריאה"""
        quality_score = 1.0
        
        # בדיקת תקינות ערכים
        quality_score *= self._check_value_validity(reading)
        
        # בדיקת עקביות עם נתונים היסטוריים
        quality_score *= self._check_consistency(reading, historical_data)
        
        # בדיקת זמן
        quality_score *= self._check_timestamp_validity(reading)
        
        return max(0.0, min(1.0, quality_score))
    
    def _check_value_validity(self, reading: SensorReading) -> float:
        """בדיקת תקינות ערכים"""
        score = 1.0
        
        # בדיקת טמפרטורה
        if not (-20 <= reading.temperature <= 50):
            score *= 0.5
        
        # בדיקת לחות
        if not (0 <= reading.humidity <= 100):
            score *= 0.5
        
        # בדיקת CO2
        if not (300 <= reading.co2 <= 5000):
            score *= 0.5
        
        # בדיקת תפוסה
        if reading.occupancy < 0:
            score *= 0.7
        
        return score
    
    def _check_consistency(self, reading: SensorReading, historical_data: DataBuffer) -> float:
        """בדיקת עקביות"""
        if len(historical_data.readings) < 3:
            return 1.0
        
        score = 1.0
        recent_readings = historical_data.get_recent_readings(10)  # 10 דקות אחרונות
        
        for param in ['temperature', 'humidity', 'co2']:
            current_value = getattr(reading, param)
            recent_values = [getattr(r, param) for r in recent_readings]
            
            if recent_values:
                mean_recent = statistics.mean(recent_values)
                max_deviation = max(abs(v - mean_recent) for v in recent_values)
                
                if max_deviation > 0:
                    current_deviation = abs(current_value - mean_recent)
                    if current_deviation > max_deviation * 3:  # חריגה גדולה
                        score *= 0.8
        
        return score
    
    def _check_timestamp_validity(self, reading: SensorReading) -> float:
        """בדיקת תקינות זמן"""
        now = datetime.now()
        time_diff = abs((reading.timestamp - now).total_seconds())
        
        # זמן חריג (יותר מ-10 דקות הפרש)
        if time_diff > 600:
            return 0.7
        
        return 1.0

class EdgeProcessor:
    """מעבד Edge ראשי"""
    
    def __init__(self):
        self.config = SystemConfig()
        self.data_buffers = {}  # מאגר לכל חיישן
        self.anomaly_detector = AnomalyDetector()
        self.data_filter = DataFilter()
        self.quality_assessment = QualityAssessment()
        
        # MQTT
        self.mqtt_client = mqtt.Client(client_id="hvac_edge_processor")
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_message = self._on_mqtt_message
        
        # מחשבי מגמות
        self.trend_calculators = {}
        
        # התראות
        self.active_alerts = {}
        self.alert_callbacks = []
        
        print("🖥️ מעבד Edge אותחל בהצלחה")
    
    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """חיבור ל-MQTT"""
        if rc == 0:
            # הרשמה לנושאי חיישנים
            client.subscribe("hvac/sensors/+/data")
            print("✅ מעבד Edge התחבר ל-MQTT")
        else:
            print(f"❌ כשל בחיבור Edge ל-MQTT: {rc}")
    
    def _on_mqtt_message(self, client, userdata, msg):
        """קבלת הודעה מ-MQTT"""
        try:
            # פענוח הודעה
            data = json.loads(msg.payload.decode())
            
            # יצירת אובייקט קריאה
            reading = SensorReading(
                sensor_id=data['sensor_id'],
                location=data['location'],
                timestamp=datetime.fromisoformat(data['timestamp']),
                temperature=data['data']['temperature'],
                humidity=data['data']['humidity'],
                co2=data['data']['co2'],
                air_quality_index=data['data']['air_quality_index'],
                occupancy=data['data']['occupancy'],
                system_status=data['system_status']
            )
            
            # עיבוד הקריאה
            self.process_reading(reading)
            
        except Exception as e:
            print(f"❌ שגיאה בעיבוד הודעת MQTT: {e}")
    
    def process_reading(self, reading: SensorReading) -> ProcessedReading:
        """עיבוד קריאת חיישן"""
        sensor_id = reading.sensor_id
        
        # יצירת מאגר אם לא קיים
        if sensor_id not in self.data_buffers:
            self.data_buffers[sensor_id] = DataBuffer()
        
        buffer = self.data_buffers[sensor_id]
        
        # הוספת הקריאה למאגר
        buffer.add_reading(reading)
        
        # זיהוי חריגות
        anomaly_score, anomalies = self.anomaly_detector.detect_anomaly(reading, buffer)
        
        # סינון נתונים
        filtered_values = self.data_filter.filter_reading(reading)
        
        # הערכת איכות
        quality_score = self.quality_assessment.assess_quality(reading, buffer)
        
        # חישוב מגמות
        trend_indicators = self._calculate_trends(buffer)
        
        # יצירת המלצות
        recommendations = self._generate_recommendations(reading, anomalies, trend_indicators)
        
        # יצירת קריאה מעובדת
        processed = ProcessedReading(
            original_reading=reading,
            processed_timestamp=datetime.now(),
            anomaly_score=anomaly_score,
            quality_score=quality_score,
            filtered_values=filtered_values,
            trend_indicators=trend_indicators,
            recommendations=recommendations
        )
        
        # בדיקת התראות
        self._check_alerts(processed)
        
        # שליחת נתונים מעובדים לענן
        self._send_to_cloud(processed)
        
        return processed
    
    def _calculate_trends(self, buffer: DataBuffer) -> Dict[str, str]:
        """חישוב מגמות"""
        trends = {}
        
        for param in ['temperature', 'humidity', 'co2']:
            trend = self.anomaly_detector.detect_trend_anomaly(buffer, param)
            trends[param] = trend
        
        return trends
    
    def _generate_recommendations(self, reading: SensorReading, anomalies: Dict[str, bool], trends: Dict[str, str]) -> List[str]:
        """יצירת המלצות"""
        recommendations = []
        
        # המלצות לפי חריגות
        if anomalies.get('temperature', False):
            if reading.temperature > self.config.THRESHOLDS['temperature']['max']:
                recommendations.append("הפעל קירור נוסף")
            else:
                recommendations.append("הפעל חימום נוסף")
        
        if anomalies.get('humidity', False):
            if reading.humidity > self.config.THRESHOLDS['humidity']['max']:
                recommendations.append("הפעל אוורור להפחתת לחות")
            else:
                recommendations.append("הוסף לחות לאוויר")
        
        if anomalies.get('co2', False):
            recommendations.append("הגבר אוורור לפינוי CO₂")
        
        # המלצות לפי מגמות
        if trends.get('temperature') == 'rising_fast':
            recommendations.append("מגמת חימום מהירה - בדוק מערכת קירור")
        
        if trends.get('co2') == 'rising_fast':
            recommendations.append("רמת CO₂ עולה מהר - הגבר אוורור מיידית")
        
        # המלצות לפי תפוסה
        if reading.occupancy > 0:
            capacity_ratio = reading.occupancy / self.config.SENSOR_LOCATIONS.get(
                reading.sensor_id.replace('hvac_', ''), {}
            ).get('capacity', 1)
            
            if capacity_ratio > 0.8:
                recommendations.append("תפוסה גבוהה - הגבר קירור ואוורור")
        
        return recommendations
    
    def _check_alerts(self, processed: ProcessedReading):
        """בדיקת התראות"""
        reading = processed.original_reading
        alerts = []
        
        # בדיקת ספי התראות
        thresholds = self.config.THRESHOLDS
        
        # התראות טמפרטורה
        if reading.temperature <= thresholds['temperature']['critical_min']:
            alerts.append(self._create_alert(reading, 'CRITICAL', 'TEMPERATURE_TOO_LOW'))
        elif reading.temperature >= thresholds['temperature']['critical_max']:
            alerts.append(self._create_alert(reading, 'CRITICAL', 'TEMPERATURE_TOO_HIGH'))
        elif reading.temperature < thresholds['temperature']['min']:
            alerts.append(self._create_alert(reading, 'HIGH', 'TEMPERATURE_LOW'))
        elif reading.temperature > thresholds['temperature']['max']:
            alerts.append(self._create_alert(reading, 'HIGH', 'TEMPERATURE_HIGH'))
        
        # התראות לחות
        if reading.humidity <= thresholds['humidity']['critical_min']:
            alerts.append(self._create_alert(reading, 'CRITICAL', 'HUMIDITY_TOO_LOW'))
        elif reading.humidity >= thresholds['humidity']['critical_max']:
            alerts.append(self._create_alert(reading, 'CRITICAL', 'HUMIDITY_TOO_HIGH'))
        
        # התראות CO2
        if reading.co2 >= thresholds['co2']['critical_max']:
            alerts.append(self._create_alert(reading, 'CRITICAL', 'CO2_CRITICAL'))
        elif reading.co2 > thresholds['co2']['max']:
            alerts.append(self._create_alert(reading, 'HIGH', 'CO2_HIGH'))
        
        # התראות חריגות
        if processed.anomaly_score > 0.7:
            alerts.append(self._create_alert(reading, 'MEDIUM', 'ANOMALY_DETECTED'))
        
        # התראות איכות נתונים
        if processed.quality_score < 0.5:
            alerts.append(self._create_alert(reading, 'LOW', 'DATA_QUALITY_LOW'))
        
        # שליחת התראות
        for alert in alerts:
            self._send_alert(alert)
    
    def _create_alert(self, reading: SensorReading, severity: str, alert_type: str) -> Alert:
        """יצירת התראה"""
        alert_id = f"{reading.sensor_id}_{alert_type}_{int(time.time())}"
        
        messages = {
            'TEMPERATURE_TOO_LOW': f"טמפרטורה נמוכה מדי: {reading.temperature}°C",
            'TEMPERATURE_TOO_HIGH': f"טמפרטורה גבוהה מדי: {reading.temperature}°C",
            'TEMPERATURE_LOW': f"טמפרטורה נמוכה: {reading.temperature}°C",
            'TEMPERATURE_HIGH': f"טמפרטורה גבוהה: {reading.temperature}°C",
            'HUMIDITY_TOO_LOW': f"לחות נמוכה מדי: {reading.humidity}%",
            'HUMIDITY_TOO_HIGH': f"לחות גבוהה מדי: {reading.humidity}%",
            'CO2_CRITICAL': f"רמת CO₂ קריטית: {reading.co2} ppm",
            'CO2_HIGH': f"רמת CO₂ גבוהה: {reading.co2} ppm",
            'ANOMALY_DETECTED': "זוהתה חריגה סטטיסטית בנתונים",
            'DATA_QUALITY_LOW': "איכות נתונים נמוכה"
        }
        
        actions = {
            'TEMPERATURE_TOO_LOW': "הפעל חימום מיידי ובדוק מערכת",
            'TEMPERATURE_TOO_HIGH': "הפעל קירור מיידי ובדוק מערכת",
            'CO2_CRITICAL': "הגבר אוורור מיידית ופנה יועץ",
            'ANOMALY_DETECTED': "בדוק חיישן ומערכת HVAC"
        }
        
        return Alert(
            alert_id=alert_id,
            sensor_id=reading.sensor_id,
            location=reading.location,
            alert_type=alert_type,
            severity=severity,
            message=messages.get(alert_type, "התראה כללית"),
            timestamp=datetime.now(),
            values={
                'temperature': reading.temperature,
                'humidity': reading.humidity,
                'co2': reading.co2
            },
            threshold_violated=alert_type,
            recommended_action=actions.get(alert_type, "בדוק מערכת")
        )
    
    def _send_alert(self, alert: Alert):
        """שליחת התראה"""
        # שמירת התראה פעילה
        self.active_alerts[alert.alert_id] = alert
        
        # שליחה ל-MQTT
        alert_data = asdict(alert)
        alert_data['timestamp'] = alert.timestamp.isoformat()
        
        if hasattr(self, 'mqtt_client') and self.mqtt_client.is_connected():
            self.mqtt_client.publish("hvac/alerts", json.dumps(alert_data))
        
        # קריאה ל-callbacks
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                print(f"❌ שגיאה ב-callback התראה: {e}")
        
        print(f"🚨 התראה: {alert.severity} - {alert.message} ב{alert.location}")
    
    def _send_to_cloud(self, processed: ProcessedReading):
        """שליחת נתונים מעובדים לענן"""
        if hasattr(self, 'mqtt_client') and self.mqtt_client.is_connected():
            cloud_data = {
                'sensor_id': processed.original_reading.sensor_id,
                'location': processed.original_reading.location,
                'timestamp': processed.processed_timestamp.isoformat(),
                'processed_data': {
                    'filtered_values': processed.filtered_values,
                    'anomaly_score': processed.anomaly_score,
                    'quality_score': processed.quality_score,
                    'trends': processed.trend_indicators
                },
                'original_data': {
                    'temperature': processed.original_reading.temperature,
                    'humidity': processed.original_reading.humidity,
                    'co2': processed.original_reading.co2,
                    'occupancy': processed.original_reading.occupancy
                },
                'recommendations': processed.recommendations
            }
            
            self.mqtt_client.publish("hvac/cloud/processed", json.dumps(cloud_data))
    
    def start(self, mqtt_broker: str = "localhost", mqtt_port: int = 1883):
        """הפעלת מעבד Edge"""
        try:
            self.mqtt_client.connect(mqtt_broker, mqtt_port, 60)
            self.mqtt_client.loop_start()
            print("🚀 מעבד Edge הופעל בהצלחה")
            return True
        except Exception as e:
            print(f"❌ כשל בהפעלת מעבד Edge: {e}")
            return False
    
    def stop(self):
        """עצירת מעבד Edge"""
        if hasattr(self, 'mqtt_client'):
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        print("⏹️ מעבד Edge נעצר")
    
    def add_alert_callback(self, callback):
        """הוספת callback להתראות"""
        self.alert_callbacks.append(callback)
    
    def get_statistics(self) -> Dict:
        """קבלת סטטיסטיקות מעבד"""
        total_readings = sum(len(buffer.readings) for buffer in self.data_buffers.values())
        active_sensors = len(self.data_buffers)
        active_alerts_count = len(self.active_alerts)
        
        return {
            'active_sensors': active_sensors,
            'total_readings_processed': total_readings,
            'active_alerts': active_alerts_count,
            'buffers_status': {
                sensor_id: len(buffer.readings)
                for sensor_id, buffer in self.data_buffers.items()
            }
        }

# בדיקה ישירה
if __name__ == "__main__":
    print("🔧 בדיקת מעבד Edge...")
    
    # יצירת מעבד
    processor = EdgeProcessor()
    
    # הוספת callback להתראות
    def alert_handler(alert):
        print(f"📢 התראה התקבלה: {alert.severity} - {alert.message}")
    
    processor.add_alert_callback(alert_handler)
    
    # התחלת מעבד
    if processor.start():
        print("✅ מעבד Edge רץ - מחכה לנתונים...")
        
        try:
            # המתנה לנתונים
            time.sleep(30)
            
            # הדפסת סטטיסטיקות
            stats = processor.get_statistics()
            print(f"\n📊 סטטיסטיקות מעבד Edge:")
            print(f"   🔌 חיישנים פעילים: {stats['active_sensors']}")
            print(f"   📈 קריאות מעובדות: {stats['total_readings_processed']}")
            print(f"   🚨 התראות פעילות: {stats['active_alerts']}")
            
        except KeyboardInterrupt:
            print("\n⏹️ עצירה ידנית...")
        finally:
            processor.stop()
    
    print("✅ בדיקה הושלמה!")