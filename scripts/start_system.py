#!/usr/bin/env python3
"""
Smart HVAC System Startup Script
Launches all components in the correct order
"""
import os
import sys
import time
import signal
import subprocess
import threading
from datetime import datetime
import argparse

# Add project path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sensors.virtual_sensors import SensorManager
from sensors.mqtt_publisher import MQTTPublisher, HVACDataStreamer
from edge_processing.edge_processor import EdgeProcessor
from web_dashboard.app import HVACDashboardApp
from database.models import DatabaseManager

class SmartHVACSystemManager:
    """Smart HVAC System Manager"""
    
    def __init__(self):
        self.components = {}
        self.threads = {}
        self.is_running = False
        self.shutdown_requested = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        print("""
╔══════════════════════════════════════════════════════════════════╗
║                  🏢 Smart HVAC Monitoring System                 ║
║                           Version 1.0.0                         ║
╚══════════════════════════════════════════════════════════════════╝
        """)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\n🛑 Shutdown signal received ({signum})")
        self.shutdown_requested = True
        self.stop_system()
    
    def check_dependencies(self):
        """Check system dependencies"""
        print("🔍 Checking system dependencies...")
        
        required_packages = [
            'flask', 'paho-mqtt', 'sqlalchemy', 
            'numpy', 'pandas', 'scikit-learn'
        ]
        
        missing_packages = []
        for package in required_packages:
            try:
                __import__(package.replace('-', '_'))
                print(f"  ✅ {package}")
            except ImportError:
                missing_packages.append(package)
                print(f"  ❌ {package}")
        
        if missing_packages:
            print(f"\n❌ Missing packages: {', '.join(missing_packages)}")
            print("📦 Install with: pip install -r requirements.txt")
            return False
        
        print("✅ All dependencies available")
        return True
    
    def check_mqtt_broker(self):
        """Check MQTT broker availability"""
        print("🔌 Checking MQTT broker availability...")
        
        try:
            test_publisher = MQTTPublisher()
            if test_publisher.connect():
                test_publisher.disconnect()
                print("✅ MQTT broker available")
                return True
            else:
                print("⚠️ MQTT broker not available - attempting to start")
                return self.start_mqtt_broker()
        except Exception as e:
            print(f"❌ MQTT check error: {e}")
            return self.start_mqtt_broker()
    
    def start_mqtt_broker(self):
        """Start local MQTT broker"""
        print("🚀 Starting local MQTT broker...")
        
        try:
            # Try to start mosquitto
            subprocess.run(['mosquitto', '--version'], 
                         capture_output=True, check=True)
            
            # Start mosquitto in background
            broker_process = subprocess.Popen(
                ['mosquitto', '-p', '1883'],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            self.components['mqtt_broker'] = broker_process
            time.sleep(2)  # Wait for startup
            
            print("✅ MQTT broker started successfully")
            return True
            
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("⚠️ Mosquitto not installed - continuing in simulation mode")
            return True  # Continue without MQTT
    
    def initialize_database(self):
        """Initialize database"""
        print("🗄️ Initializing database...")
        
        try:
            # Create database directory if not exists
            db_dir = 'database'
            if not os.path.exists(db_dir):
                os.makedirs(db_dir)
            
            # Initialize database
            db_manager = DatabaseManager()
            db_manager.initialize_sensors()
            
            self.components['database'] = db_manager
            print("✅ Database ready")
            return True
            
        except Exception as e:
            print(f"❌ Database initialization error: {e}")
            return False
    
    def start_sensors(self):
        """Start virtual sensors"""
        print("🔌 Starting virtual sensors...")
        
        try:
            sensor_manager = SensorManager()
            self.components['sensor_manager'] = sensor_manager
            
            # Start monitoring
            sensor_manager.start_monitoring(interval=5)
            print("✅ 7 virtual sensors active")
            return True
            
        except Exception as e:
            print(f"❌ Sensor startup error: {e}")
            return False
    
    def start_mqtt_publisher(self):
        """Start MQTT publisher"""
        print("📡 Starting MQTT publisher...")
        
        try:
            mqtt_publisher = MQTTPublisher()
            if mqtt_publisher.connect():
                self.components['mqtt_publisher'] = mqtt_publisher
                
                # Create data streamer
                sensor_manager = self.components['sensor_manager']
                data_streamer = HVACDataStreamer(mqtt_publisher, sensor_manager)
                data_streamer.start_streaming(interval=5)
                
                self.components['data_streamer'] = data_streamer
                print("✅ MQTT publisher active")
                return True
            else:
                print("⚠️ MQTT publisher failed to connect")
                return False
                
        except Exception as e:
            print(f"❌ MQTT publisher error: {e}")
            return False
    
    def start_edge_processor(self):
        """Start Edge processor"""
        print("🖥️ Starting Edge processor...")
        
        try:
            edge_processor = EdgeProcessor()
            
            # Add alert callback
            def alert_callback(alert):
                print(f"🚨 Alert: {alert.severity} - {alert.message}")
            
            edge_processor.add_alert_callback(alert_callback)
            
            if edge_processor.start():
                self.components['edge_processor'] = edge_processor
                print("✅ Edge processor active")
                return True
            else:
                print("⚠️ Edge processor failed to connect")
                return False
                
        except Exception as e:
            print(f"❌ Edge processor error: {e}")
            return False
    
    def start_web_dashboard(self, host='0.0.0.0', port=5000):
        """Start web dashboard"""
        print("🌐 Starting web dashboard...")
        
        try:
            dashboard_app = HVACDashboardApp()
            self.components['dashboard'] = dashboard_app
            
            # Run in separate thread
            def run_dashboard():
                dashboard_app.run(host=host, port=port, debug=False)
            
            dashboard_thread = threading.Thread(target=run_dashboard)
            dashboard_thread.daemon = True
            dashboard_thread.start()
            
            self.threads['dashboard'] = dashboard_thread
            
            print(f"✅ Web dashboard active: http://{host}:{port}")
            return True
            
        except Exception as e:
            print(f"❌ Web dashboard error: {e}")
            return False
    
    def start_system(self, host='0.0.0.0', port=5000):
        """Start complete system"""
        print(f"🚀 Starting Smart HVAC System - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Initial checks
        if not self.check_dependencies():
            return False
        
        if not self.check_mqtt_broker():
            print("⚠️ Continuing without MQTT broker")
        
        # Startup sequence
        steps = [
            ("Initialize Database", self.initialize_database),
            ("Start Sensors", self.start_sensors),
            ("Start MQTT Publisher", self.start_mqtt_publisher),
            ("Start Edge Processor", self.start_edge_processor),
            ("Start Web Dashboard", lambda: self.start_web_dashboard(host, port))
        ]
        
        for step_name, step_func in steps:
            print(f"\n🔄 {step_name}...")
            if not step_func():
                print(f"❌ Failed: {step_name}")
                self.stop_system()
                return False
        
        self.is_running = True
        print(f"""
╔══════════════════════════════════════════════════════════════════╗
║  ✅ Smart HVAC System Started Successfully!                      ║
║                                                                  ║
║  🌐 Dashboard: http://{host}:{port}                                   ║
║  📊 Sensors: 7 active sensors                                   ║
║  🔄 Updates: Every 5 seconds                                     ║
║                                                                  ║
║  To stop: Press Ctrl+C                                          ║
╚══════════════════════════════════════════════════════════════════╝
        """)
        
        return True
    
    def stop_system(self):
        """Stop the system"""
        if not self.is_running:
            return
        
        print("\n🛑 Stopping Smart HVAC System...")
        
        # Stop components
        if 'data_streamer' in self.components:
            self.components['data_streamer'].stop_streaming()
        
        if 'edge_processor' in self.components:
            self.components['edge_processor'].stop()
        
        if 'sensor_manager' in self.components:
            self.components['sensor_manager'].stop_monitoring()
        
        if 'mqtt_publisher' in self.components:
            self.components['mqtt_publisher'].disconnect()
        
        if 'dashboard' in self.components:
            self.components['dashboard'].stop_mqtt_connection()
        
        if 'mqtt_broker' in self.components:
            self.components['mqtt_broker'].terminate()
        
        self.is_running = False
        print("✅ Smart HVAC System stopped successfully")
    
    def run_monitoring_loop(self):
        """System monitoring loop"""
        try:
            while self.is_running and not self.shutdown_requested:
                # Health check
                self.health_check()
                time.sleep(30)  # Check every 30 seconds
                
        except KeyboardInterrupt:
            print("\n⏹️ Manual stop...")
        finally:
            self.stop_system()
    
    def health_check(self):
        """System health check"""
        components_status = {}
        
        # Check sensors
        if 'sensor_manager' in self.components:
            sensor_manager = self.components['sensor_manager']
            components_status['sensors'] = len(sensor_manager.sensors)
        
        # Check MQTT
        if 'mqtt_publisher' in self.components:
            mqtt_publisher = self.components['mqtt_publisher']
            components_status['mqtt'] = mqtt_publisher.is_connected
        
        # Check Edge
        if 'edge_processor' in self.components:
            edge_processor = self.components['edge_processor']
            stats = edge_processor.get_statistics()
            components_status['edge'] = stats
        
        # Print status
        current_time = datetime.now().strftime('%H:%M:%S')
        print(f"💓 {current_time} - System running normally")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Smart HVAC Monitoring System')
    parser.add_argument('--host', default='0.0.0.0', help='Server host (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=5000, help='Server port (default: 5000)')
    parser.add_argument('--no-mqtt', action='store_true', help='Run without MQTT')
    parser.add_argument('--demo', action='store_true', help='Demo mode')
    
    args = parser.parse_args()
    
    # Create system manager
    system_manager = SmartHVACSystemManager()
    
    try:
        # Start the system
        if system_manager.start_system(host=args.host, port=args.port):
            # Run monitoring loop
            system_manager.run_monitoring_loop()
        else:
            print("❌ System startup failed")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        system_manager.stop_system()
        sys.exit(1)

if __name__ == "__main__":
    main()#!/usr/bin/env python3
"""
סקריפט הפעלת מערכת HVAC מלאה
מפעיל את כל הרכיבים בסדר הנכון
"""
import os
import sys
import time
import signal
import subprocess
import threading
from datetime import datetime
import argparse

# הוספת נתיב הפרויקט
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sensors.virtual_sensors import SensorManager
from sensors.mqtt_publisher import MQTTPublisher, HVACDataStreamer
from edge_processing.edge_processor import EdgeProcessor
from web_dashboard.app import HVACDashboardApp
from database.models import DatabaseManager

class HVACSystemManager:
    """מנהל מערכת HVAC מלאה"""
    
    def __init__(self):
        self.components = {}
        self.threads = {}
        self.is_running = False
        self.shutdown_requested = False
        
        # הגדרת signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        print("""
╔══════════════════════════════════════════════════════════════════╗
║                    🏢 מערכת ניטור HVAC חכמה                      ║
║                        גרסה 1.0.0                                ║
╚══════════════════════════════════════════════════════════════════╝
        """)
    
    def _signal_handler(self, signum, frame):
        """טיפול בסיגנלי כיבוי"""
        print(f"\n🛑 התקבל סיגנל כיבוי ({signum})")
        self.shutdown_requested = True
        self.stop_system()
    
    def check_dependencies(self):
        """בדיקת תלויות מערכת"""
        print("🔍 בודק תלויות מערכת...")
        
        required_packages = [
            'flask', 'paho-mqtt', 'sqlalchemy', 
            'numpy', 'pandas', 'scikit-learn'
        ]
        
        missing_packages = []
        for package in required_packages:
            try:
                __import__(package.replace('-', '_'))
                print(f"  ✅ {package}")
            except ImportError:
                missing_packages.append(package)
                print(f"  ❌ {package}")
        
        if missing_packages:
            print(f"\n❌ חסרים חבילות: {', '.join(missing_packages)}")
            print("📦 התקן עם: pip install -r requirements.txt")
            return False
        
        print("✅ כל התלויות זמינות")
        return True
    
    def check_mqtt_broker(self):
        """בדיקת זמינות MQTT broker"""
        print("🔌 בודק זמינות MQTT broker...")
        
        try:
            test_publisher = MQTTPublisher()
            if test_publisher.connect():
                test_publisher.disconnect()
                print("✅ MQTT broker זמין")
                return True
            else:
                print("⚠️ MQTT broker לא זמין - ינוסה להפעיל")
                return self.start_mqtt_broker()
        except Exception as e:
            print(f"❌ שגיאה בבדיקת MQTT: {e}")
            return self.start_mqtt_broker()
    
    def start_mqtt_broker(self):
        """הפעלת MQTT broker מקומי"""
        print("🚀 מפעיל MQTT broker מקומי...")
        
        try:
            # ניסיון להפעיל mosquitto
            subprocess.run(['mosquitto', '--version'], 
                         capture_output=True, check=True)
            
            # הפעלת mosquitto ברקע
            broker_process = subprocess.Popen(
                ['mosquitto', '-p', '1883'],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            self.components['mqtt_broker'] = broker_process
            time.sleep(2)  # המתנה להפעלה
            
            print("✅ MQTT broker הופעל בהצלחה")
            return True
            
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("⚠️ Mosquitto לא מותקן - משתמש במצב סימולציה")
            return True  # ממשיך בלי MQTT
    
    def initialize_database(self):
        """אתחול בסיס נתונים"""
        print("🗄️ מאתחל בסיס נתונים...")
        
        try:
            # יצירת תיקיית database אם לא קיימת
            db_dir = 'database'
            if not os.path.exists(db_dir):
                os.makedirs(db_dir)
            
            # אתחול בסיס נתונים
            db_manager = DatabaseManager()
            db_manager.initialize_sensors()
            
            self.components['database'] = db_manager
            print("✅ בסיס נתונים מוכן")
            return True
            
        except Exception as e:
            print(f"❌ שגיאה באתחול בסיס נתונים: {e}")
            return False
    
    def start_sensors(self):
        """הפעלת החיישנים הוירטואליים"""
        print("🔌 מפעיל חיישנים וירטואליים...")
        
        try:
            sensor_manager = SensorManager()
            self.components['sensor_manager'] = sensor_manager
            
            # הפעלת ניטור
            sensor_manager.start_monitoring(interval=5)
            print("✅ 7 חיישנים וירטואליים פעילים")
            return True
            
        except Exception as e:
            print(f"❌ שגיאה בהפעלת חיישנים: {e}")
            return False
    
    def start_mqtt_publisher(self):
        """הפעלת מפיץ MQTT"""
        print("📡 מפעיל מפיץ MQTT...")
        
        try:
            mqtt_publisher = MQTTPublisher()
            if mqtt_publisher.connect():
                self.components['mqtt_publisher'] = mqtt_publisher
                
                # יצירת data streamer
                sensor_manager = self.components['sensor_manager']
                data_streamer = HVACDataStreamer(mqtt_publisher, sensor_manager)
                data_streamer.start_streaming(interval=5)
                
                self.components['data_streamer'] = data_streamer
                print("✅ מפיץ MQTT פעיל")
                return True
            else:
                print("⚠️ מפיץ MQTT לא הצליח להתחבר")
                return False
                
        except Exception as e:
            print(f"❌ שגיאה במפיץ MQTT: {e}")
            return False
    
    def start_edge_processor(self):
        """הפעלת מעבד Edge"""
        print("🖥️ מפעיל מעבד Edge...")
        
        try:
            edge_processor = EdgeProcessor()
            
            # הוספת callback להתראות
            def alert_callback(alert):
                print(f"🚨 התראה: {alert.severity} - {alert.message}")
            
            edge_processor.add_alert_callback(alert_callback)
            
            if edge_processor.start():
                self.components['edge_processor'] = edge_processor
                print("✅ מעבד Edge פעיל")
                return True
            else:
                print("⚠️ מעבד Edge לא הצליח להתחבר")
                return False
                
        except Exception as e:
            print(f"❌ שגיאה במעבד Edge: {e}")
            return False
    
    def start_web_dashboard(self, host='0.0.0.0', port=5000):
        """הפעלת דשבורד ווב"""
        print("🌐 מפעיל דשבורד ווב...")
        
        try:
            dashboard_app = HVACDashboardApp()
            self.components['dashboard'] = dashboard_app
            
            # הפעלה ב-thread נפרד
            def run_dashboard():
                dashboard_app.run(host=host, port=port, debug=False)
            
            dashboard_thread = threading.Thread(target=run_dashboard)
            dashboard_thread.daemon = True
            dashboard_thread.start()
            
            self.threads['dashboard'] = dashboard_thread
            
            print(f"✅ דשבורד ווב פעיל: http://{host}:{port}")
            return True
            
        except Exception as e:
            print(f"❌ שגיאה בדשבורד ווב: {e}")
            return False
    
    def start_system(self, host='0.0.0.0', port=5000):
        """הפעלת מערכת מלאה"""
        print(f"🚀 מתחיל הפעלת מערכת HVAC - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # בדיקות ראשוניות
        if not self.check_dependencies():
            return False
        
        if not self.check_mqtt_broker():
            print("⚠️ ממשיך ללא MQTT broker")
        
        # רצף הפעלה
        steps = [
            ("אתחול בסיס נתונים", self.initialize_database),
            ("הפעלת חיישנים", self.start_sensors),
            ("הפעלת מפיץ MQTT", self.start_mqtt_publisher),
            ("הפעלת מעבד Edge", self.start_edge_processor),
            ("הפעלת דשבורד ווב", lambda: self.start_web_dashboard(host, port))
        ]
        
        for step_name, step_func in steps:
            print(f"\n🔄 {step_name}...")
            if not step_func():
                print(f"❌ כשל ב{step_name}")
                self.stop_system()
                return False
        
        self.is_running = True
        print(f"""
╔══════════════════════════════════════════════════════════════════╗
║  ✅ מערכת HVAC הופעלה בהצלחה!                                    ║
║                                                                  ║
║  🌐 דשבורד: http://{host}:{port}                                   ║
║  📊 חיישנים: 7 פעילים                                            ║
║  🔄 עדכונים: כל 5 שניות                                          ║
║                                                                  ║
║  לעצירה: Ctrl+C                                                 ║
╚══════════════════════════════════════════════════════════════════╝
        """)
        
        return True
    
    def stop_system(self):
        """עצירת המערכת"""
        if not self.is_running:
            return
        
        print("\n🛑 עוצר מערכת HVAC...")
        
        # עצירת רכיבים
        if 'data_streamer' in self.components:
            self.components['data_streamer'].stop_streaming()
        
        if 'edge_processor' in self.components:
            self.components['edge_processor'].stop()
        
        if 'sensor_manager' in self.components:
            self.components['sensor_manager'].stop_monitoring()
        
        if 'mqtt_publisher' in self.components:
            self.components['mqtt_publisher'].disconnect()
        
        if 'dashboard' in self.components:
            self.components['dashboard'].stop_mqtt_connection()
        
        if 'mqtt_broker' in self.components:
            self.components['mqtt_broker'].terminate()
        
        self.is_running = False
        print("✅ מערכת HVAC נעצרה בהצלחה")
    
    def run_monitoring_loop(self):
        """לולאת ניטור המערכת"""
        try:
            while self.is_running and not self.shutdown_requested:
                # בדיקת בריאות רכיבים
                self.health_check()
                time.sleep(30)  # בדיקה כל 30 שניות
                
        except KeyboardInterrupt:
            print("\n⏹️ עצירה ידנית...")
        finally:
            self.stop_system()
    
    def health_check(self):
        """בדיקת בריאות המערכת"""
        components_status = {}
        
        # בדיקת חיישנים
        if 'sensor_manager' in self.components:
            sensor_manager = self.components['sensor_manager']
            components_status['sensors'] = len(sensor_manager.sensors)
        
        # בדיקת MQTT
        if 'mqtt_publisher' in self.components:
            mqtt_publisher = self.components['mqtt_publisher']
            components_status['mqtt'] = mqtt_publisher.is_connected
        
        # בדיקת Edge
        if 'edge_processor' in self.components:
            edge_processor = self.components['edge_processor']
            stats = edge_processor.get_statistics()
            components_status['edge'] = stats
        
        # הדפסת סטטוס (אם נדרש)
        current_time = datetime.now().strftime('%H:%M:%S')
        print(f"💓 {current_time} - מערכת פעילה")

def main():
    """פונקציה ראשית"""
    parser = argparse.ArgumentParser(description='מערכת ניטור HVAC חכמה')
    parser.add_argument('--host', default='0.0.0.0', help='כתובת שרת (ברירת מחדל: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=5000, help='פורט שרת (ברירת מחדל: 5000)')
    parser.add_argument('--no-mqtt', action='store_true', help='הפעלה ללא MQTT')
    parser.add_argument('--demo', action='store_true', help='מצב הדגמה')
    
    args = parser.parse_args()
    
    # יצירת מנהל מערכת
    system_manager = HVACSystemManager()
    
    try:
        # הפעלת המערכת
        if system_manager.start_system(host=args.host, port=args.port):
            # לולאת ניטור
            system_manager.run_monitoring_loop()
        else:
            print("❌ כשל בהפעלת המערכת")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ שגיאה לא צפויה: {e}")
        system_manager.stop_system()
        sys.exit(1)

if __name__ == "__main__":
    main()