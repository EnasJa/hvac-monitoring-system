"""
Logging System for HVAC Monitoring
מערכת לוגים למערכת HVAC
"""
import logging
import logging.handlers
import os
import sys
from datetime import datetime
from typing import Optional
import json

class HVACLogger:
    """מערכת לוגים למערכת HVAC"""
    
    def __init__(self, name: str = "hvac_system", log_level: str = "INFO"):
        self.name = name
        self.log_level = getattr(logging, log_level.upper())
        self.logger = None
        self._setup_logger()
    
    def _setup_logger(self):
        """הגדרת מערכת הלוגים"""
        # יצירת logger
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(self.log_level)
        
        # מניעת כפילות
        if self.logger.handlers:
            return
        
        # יצירת תיקיית לוגים
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # פורמט לוגים
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Console Handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # File Handler - כללי
        file_handler = logging.handlers.RotatingFileHandler(
            os.path.join(log_dir, f"{self.name}.log"),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Error File Handler
        error_handler = logging.handlers.RotatingFileHandler(
            os.path.join(log_dir, f"{self.name}_errors.log"),
            maxBytes=5*1024*1024,  # 5MB
            backupCount=3
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        self.logger.addHandler(error_handler)
    
    def get_logger(self):
        """החזרת אובייקט הlogger"""
        return self.logger
    
    def log_sensor_data(self, sensor_id: str, data: dict):
        """לוג נתוני חיישן"""
        self.logger.debug(f"📊 Sensor {sensor_id}: {data}")
    
    def log_alert(self, alert_type: str, severity: str, message: str, sensor_id: str = None):
        """לוג התראה"""
        alert_info = f"🚨 Alert [{severity}] {alert_type}: {message}"
        if sensor_id:
            alert_info += f" (Sensor: {sensor_id})"
        
        if severity in ['CRITICAL', 'HIGH']:
            self.logger.error(alert_info)
        elif severity == 'MEDIUM':
            self.logger.warning(alert_info)
        else:
            self.logger.info(alert_info)
    
    def log_system_event(self, event: str, details: dict = None):
        """לוג אירוע מערכת"""
        event_info = f"🔧 System Event: {event}"
        if details:
            event_info += f" | Details: {details}"
        self.logger.info(event_info)
    
    def log_performance(self, operation: str, duration_ms: float, success: bool = True):
        """לוג ביצועים"""
        status = "✅" if success else "❌"
        self.logger.debug(f"{status} {operation} completed in {duration_ms:.2f}ms")
    
    def log_database_operation(self, operation: str, table: str, success: bool = True, error: str = None):
        """לוג פעולות בסיס נתונים"""
        if success:
            self.logger.debug(f"💾 DB: {operation} on {table} - Success")
        else:
            self.logger.error(f"💾 DB: {operation} on {table} - Failed: {error}")

class StructuredLogger:
    """לוגר מובנה ל-JSON"""
    
    def __init__(self, name: str = "hvac_structured"):
        self.name = name
        self.logger = logging.getLogger(name)
        self._setup_structured_logger()
    
    def _setup_structured_logger(self):
        """הגדרת לוגר מובנה"""
        if self.logger.handlers:
            return
        
        # יצירת תיקיית לוגים
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # File Handler ל-JSON
        json_handler = logging.handlers.RotatingFileHandler(
            os.path.join(log_dir, "hvac_structured.json"),
            maxBytes=20*1024*1024,  # 20MB
            backupCount=5
        )
        json_handler.setLevel(logging.INFO)
        json_formatter = JsonFormatter()
        json_handler.setFormatter(json_formatter)
        self.logger.addHandler(json_handler)
        self.logger.setLevel(logging.INFO)
    
    def log_structured(self, event_type: str, data: dict):
        """לוג מובנה"""
        structured_data = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            **data
        }
        self.logger.info(json.dumps(structured_data, ensure_ascii=False))

class JsonFormatter(logging.Formatter):
    """פורמטר JSON"""
    
    def format(self, record):
        log_obj = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage()
        }
        
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_obj, ensure_ascii=False)

class MetricsLogger:
    """לוגר מטריקות וביצועים"""
    
    def __init__(self, name: str = "hvac_metrics"):
        self.name = name
        self.logger = logging.getLogger(name)
        self.structured_logger = StructuredLogger(f"{name}_structured")
        self._setup_metrics_logger()
    
    def _setup_metrics_logger(self):
        """הגדרת לוגר מטריקות"""
        if self.logger.handlers:
            return
        
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Metrics File Handler
        metrics_handler = logging.handlers.RotatingFileHandler(
            os.path.join(log_dir, "hvac_metrics.log"),
            maxBytes=50*1024*1024,  # 50MB
            backupCount=10
        )
        metrics_handler.setLevel(logging.INFO)
        
        # פורמט מטריקות
        metrics_formatter = logging.Formatter(
            '%(asctime)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        metrics_handler.setFormatter(metrics_formatter)
        self.logger.addHandler(metrics_handler)
        self.logger.setLevel(logging.INFO)
    
    def log_sensor_metrics(self, sensor_id: str, metrics: dict):
        """לוג מטריקות חיישן"""
        # לוג רגיל
        self.logger.info(f"SENSOR_METRICS | {sensor_id} | {metrics}")
        
        # לוג מובנה
        self.structured_logger.log_structured("sensor_metrics", {
            "sensor_id": sensor_id,
            "metrics": metrics
        })
    
    def log_system_metrics(self, metrics: dict):
        """לוג מטריקות מערכת"""
        # לוג רגיל
        self.logger.info(f"SYSTEM_METRICS | {metrics}")
        
        # לוג מובנה
        self.structured_logger.log_structured("system_metrics", metrics)
    
    def log_performance_metrics(self, component: str, operation: str, 
                               duration_ms: float, cpu_usage: float = None, 
                               memory_usage: float = None):
        """לוג מטריקות ביצועים"""
        metrics = {
            "component": component,
            "operation": operation,
            "duration_ms": duration_ms
        }
        
        if cpu_usage is not None:
            metrics["cpu_usage"] = cpu_usage
        if memory_usage is not None:
            metrics["memory_usage"] = memory_usage
        
        # לוג רגיל
        self.logger.info(f"PERFORMANCE | {component}.{operation} | {duration_ms:.2f}ms")
        
        # לוג מובנה
        self.structured_logger.log_structured("performance_metrics", metrics)
    
    def log_alert_metrics(self, alert_type: str, severity: str, 
                         sensor_id: str, response_time_ms: float = None):
        """לוג מטריקות התראות"""
        metrics = {
            "alert_type": alert_type,
            "severity": severity,
            "sensor_id": sensor_id
        }
        
        if response_time_ms is not None:
            metrics["response_time_ms"] = response_time_ms
        
        # לוג רגיל
        self.logger.info(f"ALERT_METRICS | {alert_type} | {severity} | {sensor_id}")
        
        # לוג מובנה
        self.structured_logger.log_structured("alert_metrics", metrics)

# Global loggers
_main_logger = None
_metrics_logger = None

def get_logger(name: str = "hvac_system", log_level: str = "INFO") -> logging.Logger:
    """קבלת logger ראשי"""
    global _main_logger
    if _main_logger is None:
        _main_logger = HVACLogger(name, log_level)
    return _main_logger.get_logger()

def get_metrics_logger() -> MetricsLogger:
    """קבלת metrics logger"""
    global _metrics_logger
    if _metrics_logger is None:
        _metrics_logger = MetricsLogger()
    return _metrics_logger

def setup_logging(log_level: str = "INFO", enable_metrics: bool = True):
    """הגדרה כללית של מערכת הלוגים"""
    # הגדרת logger ראשי
    main_logger = get_logger("hvac_system", log_level)
    
    # הגדרת metrics logger אם נדרש
    if enable_metrics:
        metrics_logger = get_metrics_logger()
    
    main_logger.info("🔧 Logging system initialized")
    return main_logger

# דקורטור ללוגים
def log_function_call(logger_name: str = "hvac_system"):
    """דקורטור ללוג קריאות פונקציות"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = get_logger(logger_name)
            start_time = datetime.now()
            
            try:
                logger.debug(f"🔄 Starting {func.__name__}")
                result = func(*args, **kwargs)
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds() * 1000
                logger.debug(f"✅ Completed {func.__name__} in {duration:.2f}ms")
                
                return result
            except Exception as e:
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds() * 1000
                logger.error(f"❌ Failed {func.__name__} after {duration:.2f}ms: {e}")
                raise
        
        return wrapper
    return decorator

# Context manager ללוגים
class LogContext:
    """Context manager ללוגים"""
    
    def __init__(self, operation: str, logger_name: str = "hvac_system"):
        self.operation = operation
        self.logger = get_logger(logger_name)
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.debug(f"🔄 Starting {self.operation}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds() * 1000
        
        if exc_type is None:
            self.logger.debug(f"✅ Completed {self.operation} in {duration:.2f}ms")
        else:
            self.logger.error(f"❌ Failed {self.operation} after {duration:.2f}ms: {exc_val}")

# בדיקה ישירה
if __name__ == "__main__":
    print("🔧 Testing Logging System...")
    
    # הגדרת מערכת לוגים
    logger = setup_logging("DEBUG", enable_metrics=True)
    metrics_logger = get_metrics_logger()
    
    # בדיקות לוגים רגילים
    logger.info("🧪 Testing main logger")
    logger.debug("Debug message")
    logger.warning("Warning message")
    logger.error("Error message")
    
    # בדיקת לוגי חיישנים
    hvac_logger = HVACLogger("test_hvac")
    hvac_logger.log_sensor_data("sensor_001", {
        "temperature": 23.5,
        "humidity": 45.2,
        "co2": 450
    })
    
    # בדיקת לוגי התראות
    hvac_logger.log_alert("TEMPERATURE_HIGH", "MEDIUM", "Temperature above threshold", "sensor_001")
    
    # בדיקת מטריקות
    metrics_logger.log_sensor_metrics("sensor_001", {
        "avg_temp": 23.5,
        "readings_count": 100,
        "quality_score": 0.95
    })
    
    metrics_logger.log_performance_metrics("edge_processor", "process_reading", 15.5, 12.3, 45.7)
    
    # בדיקת דקורטור
    @log_function_call("test_logger")
    def test_function(x, y):
        import time
        time.sleep(0.1)  # סימולציה של עבודה
        return x + y
    
    result = test_function(5, 3)
    print(f"Function result: {result}")
    
    # בדיקת context manager
    with LogContext("test_operation", "test_logger"):
        import time
        time.sleep(0.05)
        print("Working inside context...")
    
    print("✅ Logging system test completed!")
    print("📁 Check 'logs' directory for log files")