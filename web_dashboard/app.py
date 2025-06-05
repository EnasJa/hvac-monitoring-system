"""
אפליקציית Flask לדשבורד HVAC
"""
from flask import Flask, render_template, jsonify, request, redirect, url_for
from flask_socketio import SocketIO, emit
from flask_cors import CORS
import json
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List
import paho.mqtt.client as mqtt

from config.system_config import SystemConfig, get_config
from database.models import DatabaseManager
from sensors.virtual_sensors import SensorReading

class HVACDashboardApp:
    """אפליקציית דשבורד HVAC"""
    
    def __init__(self):
        # יצירת Flask app
        self.app = Flask(__name__)
        self.app.config.from_object(get_config())
        
        # הגדרת CORS ו-SocketIO
        CORS(self.app)
        self.socketio = SocketIO(self.app, cors_allowed_origins="*")
        
        # מנהל בסיס נתונים
        self.db_manager = DatabaseManager()
        self.db_manager.initialize_sensors()
        
        # MQTT עבור נתונים בזמן אמת
        self.mqtt_client = mqtt.Client(client_id="hvac_dashboard")
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_message = self._on_mqtt_message
        
        # נתונים זמניים
        self.realtime_data = {}
        self.latest_alerts = []
        self.system_status = "NORMAL"
        
        # הגדרת routes
        self._setup_routes()
        self._setup_socketio_events()
        
        print("🌐 דשבורד HVAC אותחל בהצלחה")
    
    def _setup_routes(self):
        """הגדרת נתיבי הווב"""
        
        @self.app.route('/')
        def index():
            """עמוד ראשי"""
            return render_template('index.html')
        
        @self.app.route('/dashboard')
        def dashboard():
            """דשבורד ראשי"""
            return render_template('dashboard.html')
        
        @self.app.route('/alerts')
        def alerts_page():
            """מסך התראות"""
            return render_template('alerts.html')
        
        # API Routes
        @self.app.route('/api/sensors')
        def get_sensors():
            """קבלת רשימת חיישנים"""
            try:
                sensors = self._get_sensors_info()
                return jsonify({
                    'success': True,
                    'data': sensors
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/data/latest')
        def get_latest_data():
            """קבלת נתונים אחרונים"""
            try:
                # קבלת נתונים מבסיס הנתונים
                readings = self.db_manager.get_latest_readings(limit=50)
                
                # עיבוד לפי חיישן
                sensors_data = {}
                for reading in readings:
                    sensor_id = reading['sensor_id']
                    if sensor_id not in sensors_data:
                        sensors_data[sensor_id] = []
                    sensors_data[sensor_id].append(reading)
                
                return jsonify({
                    'success': True,
                    'data': sensors_data,
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/data/sensor/<sensor_id>')
        def get_sensor_data(sensor_id):
            """קבלת נתונים לחיישן ספציפי"""
            try:
                hours = request.args.get('hours', 24, type=int)
                readings = self.db_manager.get_sensor_readings(sensor_id, hours)
                
                return jsonify({
                    'success': True,
                    'data': readings,
                    'sensor_id': sensor_id
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/alerts')
        def get_alerts():
            """קבלת התראות פעילות"""
            try:
                alerts = self.db_manager.get_active_alerts()
                return jsonify({
                    'success': True,
                    'data': alerts
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/alerts/<alert_id>/acknowledge', methods=['POST'])
        def acknowledge_alert(alert_id):
            """אישור התראה"""
            try:
                success = self.db_manager.acknowledge_alert(alert_id)
                if success:
                    # עדכון לקליינטים
                    self.socketio.emit('alert_acknowledged', {'alert_id': alert_id})
                    return jsonify({'success': True})
                else:
                    return jsonify({'success': False, 'error': 'Alert not found'}), 404
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/alerts/<alert_id>/resolve', methods=['POST'])
        def resolve_alert(alert_id):
            """פתרון התראה"""
            try:
                success = self.db_manager.resolve_alert(alert_id)
                if success:
                    # עדכון לקליינטים
                    self.socketio.emit('alert_resolved', {'alert_id': alert_id})
                    return jsonify({'success': True})
                else:
                    return jsonify({'success': False, 'error': 'Alert not found'}), 404
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/statistics')
        def get_statistics():
            """קבלת סטטיסטיקות מערכת"""
            try:
                stats = self.db_manager.get_system_statistics()
                return jsonify({
                    'success': True,
                    'data': stats
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/export/csv')
        def export_csv():
            """ייצוא נתונים ל-CSV"""
            try:
                hours = request.args.get('hours', 24, type=int)
                sensor_id = request.args.get('sensor_id', None)
                
                if sensor_id:
                    readings = self.db_manager.get_sensor_readings(sensor_id, hours)
                else:
                    readings = self.db_manager.get_latest_readings(limit=1000)
                
                # יצירת CSV
                csv_data = self._create_csv_from_readings(readings)
                
                from flask import Response
                return Response(
                    csv_data,
                    mimetype='text/csv',
                    headers={'Content-Disposition': f'attachment; filename=hvac_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
                )
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
    
    def _setup_socketio_events(self):
        """הגדרת אירועי SocketIO"""
        
        @self.socketio.on('connect')
        def handle_connect():
            """חיבור קליינט"""
            print(f"🔗 קליינט התחבר: {request.sid}")
            
            # שליחת נתונים ראשוניים
            emit('initial_data', {
                'sensors': self._get_sensors_info(),
                'latest_readings': self.realtime_data,
                'alerts': self.latest_alerts,
                'system_status': self.system_status
            })
        
        @self.socketio.on('disconnect')
        def handle_disconnect():
            """ניתוק קליינט"""
            print(f"🔌 קליינט התנתק: {request.sid}")
        
        @self.socketio.on('request_sensor_data')
        def handle_sensor_data_request(data):
            """בקשת נתוני חיישן"""
            sensor_id = data.get('sensor_id')
            hours = data.get('hours', 1)
            
            try:
                readings = self.db_manager.get_sensor_readings(sensor_id, hours)
                emit('sensor_data_response', {
                    'sensor_id': sensor_id,
                    'readings': readings
                })
            except Exception as e:
                emit('error', {'message': f'Error fetching sensor data: {str(e)}'})
    
    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """חיבור ל-MQTT"""
        if rc == 0:
            # הרשמה לנושאים
            client.subscribe([
                ("hvac/sensors/+/data", 0),
                ("hvac/alerts", 0),
                ("hvac/cloud/processed", 0),
                ("hvac/system/status", 0)
            ])
            print("✅ דשבורד התחבר ל-MQTT")
        else:
            print(f"❌ כשל בחיבור דשבורד ל-MQTT: {rc}")
    
    def _on_mqtt_message(self, client, userdata, msg):
        """קבלת הודעה מ-MQTT"""
        try:
            topic = msg.topic
            data = json.loads(msg.payload.decode())
            
            if topic.startswith("hvac/sensors/") and topic.endswith("/data"):
                self._handle_sensor_data(data)
            elif topic == "hvac/alerts":
                self._handle_alert(data)
            elif topic == "hvac/cloud/processed":
                self._handle_processed_data(data)
            elif topic == "hvac/system/status":
                self._handle_system_status(data)
                
        except Exception as e:
            print(f"❌ שגיאה בעיבוד הודעת MQTT: {e}")
    
    def _handle_sensor_data(self, data):
        """עיבוד נתוני חיישן"""
        sensor_id = data['sensor_id']
        
        # עדכון נתונים בזמן אמת
        self.realtime_data[sensor_id] = data
        
        # שמירה בבסיס נתונים
        reading_data = {
            'sensor_id': sensor_id,
            'timestamp': data['timestamp'],
            'temperature': data['data']['temperature'],
            'humidity': data['data']['humidity'],
            'co2': data['data']['co2'],
            'air_quality_index': data['data']['air_quality_index'],
            'occupancy': data['data']['occupancy'],
            'system_status': data['system_status']
        }
        
        self.db_manager.save_sensor_reading(reading_data)
        
        # שליחה לקליינטים מחוברים
        self.socketio.emit('realtime_data', {
            'sensor_id': sensor_id,
            'data': data,
            'timestamp': datetime.now().isoformat()
        })
    
    def _handle_alert(self, alert_data):
        """עיבוד התראה"""
        # שמירה בבסיס נתונים
        self.db_manager.save_alert(alert_data['alert'])
        
        # עדכון רשימת התראות
        self.latest_alerts.insert(0, alert_data['alert'])
        if len(self.latest_alerts) > 50:  # שמירת 50 התראות אחרונות
            self.latest_alerts.pop()
        
        # שליחה לקליינטים
        self.socketio.emit('new_alert', alert_data['alert'])
        
        print(f"🚨 התראה חדשה: {alert_data['alert']['severity']} - {alert_data['alert']['message']}")
    
    def _handle_processed_data(self, data):
        """עיבוד נתונים מעובדים מ-Edge"""
        # עדכון נתונים מעובדים
        sensor_id = data['sensor_id']
        
        # שמירת נתונים מעובדים
        processed_reading = {
            'sensor_id': sensor_id,
            'timestamp': data['timestamp'],
            'temperature': data['original_data']['temperature'],
            'humidity': data['original_data']['humidity'],
            'co2': data['original_data']['co2'],
            'occupancy': data['original_data']['occupancy'],
            'filtered_temperature': data['processed_data']['filtered_values']['temperature'],
            'filtered_humidity': data['processed_data']['filtered_values']['humidity'],
            'filtered_co2': data['processed_data']['filtered_values']['co2'],
            'anomaly_score': data['processed_data']['anomaly_score'],
            'quality_score': data['processed_data']['quality_score']
        }
        
        self.db_manager.save_sensor_reading(processed_reading)
        
        # שליחה לקליינטים
        self.socketio.emit('processed_data', data)
    
    def _handle_system_status(self, status_data):
        """עיבוד סטטוס מערכת"""
        self.system_status = status_data['system_status']['status']
        
        # שליחה לקליינטים
        self.socketio.emit('system_status_update', status_data)
    
    def _get_sensors_info(self):
        """קבלת מידע חיישנים"""
        config = SystemConfig()
        sensors = []
        
        for location_id, location_config in config.SENSOR_LOCATIONS.items():
            sensor_info = {
                'sensor_id': f"hvac_{location_id}",
                'location_id': location_id,
                'name': location_config['name'],
                'description': location_config['description'],
                'coordinates': location_config['coordinates'],
                'capacity': location_config['capacity'],
                'latest_data': self.realtime_data.get(f"hvac_{location_id}", None)
            }
            sensors.append(sensor_info)
        
        return sensors
    
    def _create_csv_from_readings(self, readings):
        """יצירת CSV מקריאות"""
        import io
        import csv
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # כותרות
        headers = [
            'Timestamp', 'Sensor ID', 'Location', 'Temperature (°C)', 
            'Humidity (%)', 'CO2 (ppm)', 'Air Quality Index', 
            'Occupancy', 'System Status', 'Anomaly Score', 'Quality Score'
        ]
        writer.writerow(headers)
        
        # נתונים
        for reading in readings:
            row = [
                reading['timestamp'],
                reading['sensor_id'],
                reading.get('location', ''),
                reading['temperature'],
                reading['humidity'],
                reading['co2'],
                reading.get('air_quality_index', ''),
                reading.get('occupancy', ''),
                reading.get('system_status', ''),
                reading.get('anomaly_score', ''),
                reading.get('quality_score', '')
            ]
            writer.writerow(row)
        
        return output.getvalue()
    
    def start_mqtt_connection(self, broker_host="localhost", broker_port=1883):
        """התחלת חיבור MQTT"""
        try:
            self.mqtt_client.connect(broker_host, broker_port, 60)
            self.mqtt_client.loop_start()
            print(f"🔌 דשבורד התחבר ל-MQTT: {broker_host}:{broker_port}")
            return True
        except Exception as e:
            print(f"❌ כשל בחיבור דשבורד ל-MQTT: {e}")
            return False
    
    def stop_mqtt_connection(self):
        """עצירת חיבור MQTT"""
        try:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            print("🔌 חיבור MQTT של הדשבורד נותק")
        except Exception as e:
            print(f"❌ שגיאה בניתוק MQTT: {e}")
    
    def run(self, host='0.0.0.0', port=5000, debug=True):
        """הפעלת השרת"""
        print(f"""
🌐 דשבורד HVAC מתחיל...
📍 כתובת: http://{host}:{port}
🔧 מצב דיבוג: {'מופעל' if debug else 'כבוי'}
        """)
        
        # התחלת חיבור MQTT
        self.start_mqtt_connection()
        
        try:
            # הפעלת השרת
            self.socketio.run(
                self.app,
                host=host,
                port=port,
                debug=debug,
                use_reloader=False  # כדי למנוע בעיות עם MQTT
            )
        except KeyboardInterrupt:
            print("\n⏹️ עצירה ידנית...")
        finally:
            self.stop_mqtt_connection()

# פונקציות עזר
def create_app():
    """יצירת אפליקציה חדשה"""
    return HVACDashboardApp()

def run_dashboard(host='0.0.0.0', port=5000, debug=True):
    """הפעלת דשבורד"""
    app = create_app()
    app.run(host=host, port=port, debug=debug)

# בדיקה ישירה
if __name__ == "__main__":
    print("🌐 הפעלת דשבורד HVAC...")
    
    # יצירת אפליקציה
    dashboard_app = create_app()
    
    # הפעלה
    dashboard_app.run(
        host='0.0.0.0',
        port=5000,
        debug=True
    )