"""
חיישנים וירטואליים למערכת HVAC
מדמה 7 חיישנים במיקומים שונים בבניין
"""
import random
import time
import math
import json
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import threading
from dataclasses import dataclass
from config.system_config import SystemConfig

@dataclass
class SensorReading:
    """קריאת חיישן"""
    sensor_id: str
    location: str
    timestamp: datetime
    temperature: float
    humidity: float
    co2: int
    air_quality_index: float
    occupancy: int
    system_status: str

class VirtualHVACSensor:
    """חיישן HVAC וירטואלי"""
    
    def __init__(self, sensor_id: str, location_config: Dict):
        self.sensor_id = sensor_id
        self.location = location_config['name']
        self.description = location_config['description']
        self.coordinates = location_config['coordinates']
        self.capacity = location_config['capacity']
        
        # פרמטרים בסיסיים
        self.base_temperature = 22.0  # טמפרטורה בסיסית
        self.base_humidity = 45.0     # לחות בסיסית
        self.base_co2 = 400          # CO2 בסיסי
        
        # משתנים דינמיים
        self.current_occupancy = 0
        self.hvac_mode = "AUTO"
        self.last_reading_time = datetime.now()
        
        # דפוסי זמן
        self.time_patterns = self._generate_time_patterns()
        
        # מצב מערכת
        self.is_active = True
        self.malfunction_probability = 0.01  # 1% סיכוי לתקלה
        
    def _generate_time_patterns(self) -> Dict:
        """יצירת דפוסי זמן יומיים"""
        return {
            'temperature_cycle': random.uniform(0, 2 * math.pi),
            'humidity_cycle': random.uniform(0, 2 * math.pi),
            'occupancy_pattern': self._create_occupancy_pattern()
        }
    
    def _create_occupancy_pattern(self) -> Dict:
        """יצירת דפוס תפוסה יומי"""
        if self.capacity == 0:  # מסדרון או שירותים
            return {'base': 0, 'variation': 0}
        
        # דפוסים שונים לפי סוג החדר
        if 'משרד' in self.location:
            return {
                'peak_hours': [(9, 12), (14, 17)],
                'base_occupancy': 0.3,
                'peak_occupancy': 0.8
            }
        elif 'ישיבות' in self.location:
            return {
                'peak_hours': [(10, 11), (14, 15), (16, 17)],
                'base_occupancy': 0.1,
                'peak_occupancy': 0.9
            }
        else:  # מטבח, קבלה
            return {
                'peak_hours': [(12, 13), (16, 17)],
                'base_occupancy': 0.2,
                'peak_occupancy': 0.6
            }
    
    def _calculate_occupancy(self) -> int:
        """חישוב תפוסה נוכחית"""
        if self.capacity == 0:
            return 0
            
        current_hour = datetime.now().hour
        
        # בדיקת שעות עבודה
        if current_hour < 7 or current_hour > 19:
            return max(0, int(self.capacity * 0.1 * random.uniform(0.5, 1.5)))
        
        # חישוב לפי דפוס
        pattern = self.time_patterns['occupancy_pattern']
        base_rate = pattern['base_occupancy']
        
        # בדיקת שעות שיא
        is_peak = any(
            start <= current_hour <= end 
            for start, end in pattern.get('peak_hours', [])
        )
        
        if is_peak:
            occupancy_rate = pattern['peak_occupancy']
        else:
            occupancy_rate = base_rate
            
        # הוספת רעש אקראי
        occupancy_rate *= random.uniform(0.7, 1.3)
        occupancy_rate = max(0, min(1, occupancy_rate))
        
        return int(self.capacity * occupancy_rate)
    
    def _simulate_temperature(self) -> float:
        """סימולציית טמפרטורה"""
        current_time = datetime.now()
        hour = current_time.hour
        
        # מחזור יומי טבעי
        daily_variation = 2 * math.sin(2 * math.pi * hour / 24 + self.time_patterns['temperature_cycle'])
        
        # השפעת תפוסה
        occupancy_effect = self.current_occupancy * 0.3
        
        # השפעת מזג האוויר החיצוני (סימולציה)
        seasonal_temp = self._get_seasonal_temperature()
        
        # השפעת מערכת HVAC
        hvac_effect = self._get_hvac_effect()
        
        # חישוב טמפרטורה סופית
        temperature = (
            self.base_temperature + 
            daily_variation + 
            occupancy_effect + 
            seasonal_temp + 
            hvac_effect +
            random.uniform(-0.5, 0.5)  # רעש
        )
        
        return round(temperature, 1)
    
    def _simulate_humidity(self) -> float:
        """סימולציית לחות"""
        current_time = datetime.now()
        hour = current_time.hour
        
        # מחזור יומי
        daily_variation = 5 * math.sin(2 * math.pi * hour / 24 + self.time_patterns['humidity_cycle'])
        
        # השפעת תפוסה (נשימה)
        occupancy_effect = self.current_occupancy * 2
        
        # השפעת מיקום
        location_effect = 0
        if 'מטבח' in self.location:
            location_effect = 5  # לחות גבוהה יותר במטבח
        elif 'שירותים' in self.location:
            location_effect = 8
            
        # השפעת מערכת HVAC
        hvac_effect = -2 if self.hvac_mode == "COOLING" else 0
        
        humidity = (
            self.base_humidity + 
            daily_variation + 
            occupancy_effect + 
            location_effect + 
            hvac_effect +
            random.uniform(-2, 2)
        )
        
        return round(max(20, min(80, humidity)), 1)
    
    def _simulate_co2(self) -> int:
        """סימולציית רמת CO2"""
        # השפעת תפוסה (הנשמה מייצרת CO2)
        occupancy_effect = self.current_occupancy * 50
        
        # השפעת אוורור
        ventilation_effect = -50 if self.hvac_mode == "VENTILATION" else 0
        
        # הצטברות במשך הזמן
        time_since_last = (datetime.now() - self.last_reading_time).total_seconds()
        accumulation = min(100, time_since_last / 60 * 10)  # עליה של 10 ppm לדקה
        
        co2 = (
            self.base_co2 + 
            occupancy_effect + 
            ventilation_effect + 
            accumulation +
            random.randint(-20, 20)
        )
        
        return max(350, min(2000, int(co2)))
    
    def _get_seasonal_temperature(self) -> float:
        """קבלת השפעת עונה"""
        month = datetime.now().month
        
        # סימולציה של טמפרטורה חיצונית לפי חודש
        seasonal_temps = {
            12: -2, 1: -3, 2: -1,  # חורף
            3: 2, 4: 5, 5: 8,      # אביב
            6: 12, 7: 15, 8: 14,   # קיץ
            9: 8, 10: 4, 11: 0     # סתיו
        }
        
        return seasonal_temps.get(month, 0) * 0.1  # השפעה קטנה (בידוד טוב)
    
    def _get_hvac_effect(self) -> float:
        """קבלת השפעת מערכת HVAC"""
        if self.hvac_mode == "COOLING":
            return -1.5
        elif self.hvac_mode == "HEATING":
            return 1.5
        elif self.hvac_mode == "AUTO":
            # התאמה אוטומטית
            if self.current_occupancy > self.capacity * 0.6:
                return -0.5  # קירור קל
            return 0
        return 0
    
    def _calculate_air_quality_index(self, temperature: float, humidity: float, co2: int) -> float:
        """חישוב אינדקס איכות אוויר"""
        # נורמליזציה של כל פרמטר (0-100)
        temp_score = max(0, 100 - abs(temperature - 22) * 10)
        humidity_score = max(0, 100 - abs(humidity - 45) * 2)
        co2_score = max(0, 100 - (co2 - 400) / 10)
        
        # ממוצע משוקלל
        aqi = (temp_score * 0.3 + humidity_score * 0.3 + co2_score * 0.4)
        return round(aqi, 1)
    
    def _determine_system_status(self, temperature: float, humidity: float, co2: int) -> str:
        """קביעת סטטוס מערכת"""
        config = SystemConfig()
        
        # בדיקת ערכים קריטיים
        if (temperature <= config.THRESHOLDS['temperature']['critical_min'] or 
            temperature >= config.THRESHOLDS['temperature']['critical_max'] or
            humidity <= config.THRESHOLDS['humidity']['critical_min'] or
            humidity >= config.THRESHOLDS['humidity']['critical_max'] or
            co2 >= config.THRESHOLDS['co2']['critical_max']):
            return "CRITICAL"
        
        # בדיקת ערכים חריגים
        if (temperature < config.THRESHOLDS['temperature']['min'] or 
            temperature > config.THRESHOLDS['temperature']['max'] or
            humidity < config.THRESHOLDS['humidity']['min'] or
            humidity > config.THRESHOLDS['humidity']['max'] or
            co2 > config.THRESHOLDS['co2']['max']):
            return "WARNING"
        
        # הכל תקין
        return "NORMAL"
    
    def _simulate_malfunction(self) -> bool:
        """סימולציית תקלה במערכת"""
        return random.random() < self.malfunction_probability
    
    def read_sensor(self) -> SensorReading:
        """קריאת נתוני חיישן"""
        if not self.is_active:
            raise Exception(f"חיישן {self.sensor_id} לא פעיל")
        
        # עדכון תפוסה
        self.current_occupancy = self._calculate_occupancy()
        
        # סימולציית תקלה
        if self._simulate_malfunction():
            # תקלה - ערכים חריגים
            temperature = random.uniform(10, 40)
            humidity = random.uniform(10, 90)
            co2 = random.randint(300, 2500)
            system_status = "MALFUNCTION"
        else:
            # פעולה תקינה
            temperature = self._simulate_temperature()
            humidity = self._simulate_humidity()
            co2 = self._simulate_co2()
            system_status = self._determine_system_status(temperature, humidity, co2)
        
        # חישוב איכות אוויר
        air_quality_index = self._calculate_air_quality_index(temperature, humidity, co2)
        
        # עדכון זמן קריאה אחרונה
        self.last_reading_time = datetime.now()
        
        return SensorReading(
            sensor_id=self.sensor_id,
            location=self.location,
            timestamp=datetime.now(),
            temperature=temperature,
            humidity=humidity,
            co2=co2,
            air_quality_index=air_quality_index,
            occupancy=self.current_occupancy,
            system_status=system_status
        )
    
    def set_hvac_mode(self, mode: str):
        """הגדרת מצב HVAC"""
        valid_modes = ["AUTO", "COOLING", "HEATING", "VENTILATION", "OFF"]
        if mode in valid_modes:
            self.hvac_mode = mode
        else:
            raise ValueError(f"מצב לא חוקי: {mode}")
    
    def calibrate(self, temperature_offset: float = 0, humidity_offset: float = 0):
        """כיול חיישן"""
        self.base_temperature += temperature_offset
        self.base_humidity += humidity_offset

class SensorManager:
    """מנהל החיישנים הוירטואליים"""
    
    def __init__(self):
        self.sensors: Dict[str, VirtualHVACSensor] = {}
        self.is_running = False
        self.reading_thread = None
        self.callbacks = []
        
        # יצירת החיישנים
        self._initialize_sensors()
    
    def _initialize_sensors(self):
        """אתחול כל החיישנים"""
        config = SystemConfig()
        
        for location_id, location_config in config.SENSOR_LOCATIONS.items():
            sensor = VirtualHVACSensor(
                sensor_id=f"hvac_{location_id}",
                location_config=location_config
            )
            self.sensors[location_id] = sensor
        
        print(f"✅ אותחלו {len(self.sensors)} חיישנים וירטואליים")
    
    def add_callback(self, callback):
        """הוספת callback לקבלת נתונים"""
        self.callbacks.append(callback)
    
    def start_monitoring(self, interval: int = 5):
        """התחלת ניטור רציף"""
        if self.is_running:
            return
        
        self.is_running = True
        self.reading_thread = threading.Thread(
            target=self._monitoring_loop,
            args=(interval,)
        )
        self.reading_thread.daemon = True
        self.reading_thread.start()
        
        print(f"🚀 ניטור החיישנים התחיל (כל {interval} שניות)")
    
    def stop_monitoring(self):
        """עצירת ניטור"""
        self.is_running = False
        if self.reading_thread:
            self.reading_thread.join()
        print("⏹️ ניטור החיישנים נעצר")
    
    def _monitoring_loop(self, interval: int):
        """לולאת ניטור רציפה"""
        while self.is_running:
            try:
                readings = self.read_all_sensors()
                
                # שליחת נתונים לכל ה-callbacks
                for callback in self.callbacks:
                    try:
                        callback(readings)
                    except Exception as e:
                        print(f"❌ שגיאה ב-callback: {e}")
                
                time.sleep(interval)
                
            except Exception as e:
                print(f"❌ שגיאה בלולאת הניטור: {e}")
                time.sleep(1)
    
    def read_all_sensors(self) -> List[SensorReading]:
        """קריאת כל החיישנים"""
        readings = []
        
        for location_id, sensor in self.sensors.items():
            try:
                reading = sensor.read_sensor()
                readings.append(reading)
            except Exception as e:
                print(f"❌ שגיאה בקריאת חיישן {location_id}: {e}")
        
        return readings
    
    def read_sensor(self, location_id: str) -> SensorReading:
        """קריאת חיישן ספציפי"""
        if location_id not in self.sensors:
            raise ValueError(f"חיישן {location_id} לא קיים")
        
        return self.sensors[location_id].read_sensor()
    
    def get_sensor_info(self) -> Dict:
        """קבלת מידע על כל החיישנים"""
        info = {}
        
        for location_id, sensor in self.sensors.items():
            info[location_id] = {
                'sensor_id': sensor.sensor_id,
                'location': sensor.location,
                'description': sensor.description,
                'coordinates': sensor.coordinates,
                'capacity': sensor.capacity,
                'hvac_mode': sensor.hvac_mode,
                'is_active': sensor.is_active,
                'current_occupancy': sensor.current_occupancy
            }
        
        return info
    
    def set_hvac_mode_all(self, mode: str):
        """הגדרת מצב HVAC לכל החיישנים"""
        for sensor in self.sensors.values():
            sensor.set_hvac_mode(mode)
        print(f"🔧 מצב HVAC הוגדר ל-{mode} לכל החיישנים")
    
    def simulate_emergency(self, location_id: str, duration: int = 30):
        """סימולציית מצב חירום"""
        if location_id not in self.sensors:
            return
        
        sensor = self.sensors[location_id]
        
        def emergency_simulation():
            # שמירת מצב קודם
            original_malfunction_prob = sensor.malfunction_probability
            
            # הגדרת מצב חירום
            sensor.malfunction_probability = 1.0  # תקלה מובטחת
            
            print(f"🚨 מצב חירום מופעל ב-{sensor.location} למשך {duration} שניות")
            
            # המתנה
            time.sleep(duration)
            
            # החזרת מצב רגיל
            sensor.malfunction_probability = original_malfunction_prob
            print(f"✅ מצב חירום בוטל ב-{sensor.location}")
        
        # הפעלה בthread נפרד
        emergency_thread = threading.Thread(target=emergency_simulation)
        emergency_thread.daemon = True
        emergency_thread.start()

# פונקציות עזר
def create_sensor_manager() -> SensorManager:
    """יצירת מנהל חיישנים חדש"""
    return SensorManager()

def readings_to_json(readings: List[SensorReading]) -> str:
    """המרת קריאות לJSON"""
    data = []
    
    for reading in readings:
        data.append({
            'sensor_id': reading.sensor_id,
            'location': reading.location,
            'timestamp': reading.timestamp.isoformat(),
            'temperature': reading.temperature,
            'humidity': reading.humidity,
            'co2': reading.co2,
            'air_quality_index': reading.air_quality_index,
            'occupancy': reading.occupancy,
            'system_status': reading.system_status
        })
    
    return json.dumps(data, ensure_ascii=False, indent=2)

# בדיקה ישירה
if __name__ == "__main__":
    # יצירת מנהל חיישנים
    manager = create_sensor_manager()
    
    print("🔧 בדיקת החיישנים הוירטואליים...")
    
    # קריאה חד-פעמית
    readings = manager.read_all_sensors()
    
    print(f"\n📊 נתונים מ-{len(readings)} חיישנים:")
    for reading in readings:
        print(f"""
📍 {reading.location}:
   🌡️  טמפרטורה: {reading.temperature}°C
   💧 לחות: {reading.humidity}%
   🫁 CO₂: {reading.co2} ppm
   👥 תפוסה: {reading.occupancy} אנשים
   ✅ סטטוס: {reading.system_status}
   🔍 איכות אוויר: {reading.air_quality_index}
        """)
    
    # בדיקת ניטור רציף (10 שניות)
    print("\n🚀 התחלת ניטור רציף למשך 10 שניות...")
    
    def print_callback(readings):
        print(f"⏰ {datetime.now().strftime('%H:%M:%S')} - קריאה מ-{len(readings)} חיישנים")
    
    manager.add_callback(print_callback)
    manager.start_monitoring(interval=2)
    
    time.sleep(10)
    manager.stop_monitoring()
    
    print("✅ בדיקה הושלמה בהצלחה!")