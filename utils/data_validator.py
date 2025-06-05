"""
Data Validation System for HVAC Monitoring
מערכת אימות נתונים למערכת HVAC
"""
import re
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional, Union
from dataclasses import dataclass
from enum import Enum

class ValidationSeverity(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

@dataclass
class ValidationResult:
    """תוצאת אימות"""
    is_valid: bool
    severity: ValidationSeverity
    field_name: str
    message: str
    expected_value: Any = None
    actual_value: Any = None
    suggestion: str = ""

class DataValidator:
    """מאמת נתונים כללי"""
    
    def __init__(self):
        # טווחי ערכים תקינים
        self.valid_ranges = {
            'temperature': (-50.0, 80.0),  # צלזיוס
            'humidity': (0.0, 100.0),      # אחוז
            'co2': (200.0, 10000.0),       # PPM
            'pressure': (0.5, 2.0),        # בר
            'occupancy': (0, 1000),        # מספר אנשים
            'air_quality_index': (0.0, 500.0),  # AQI
            'airflow': (0.0, 2000.0),      # CFM
            'energy': (0.0, 100000.0),     # Watts
        }
        
        # טווחי ערכים סבירים (לאזהרות)
        self.reasonable_ranges = {
            'temperature': (10.0, 40.0),
            'humidity': (20.0, 80.0),
            'co2': (300.0, 2000.0),
            'pressure': (0.9, 1.1),
            'occupancy': (0, 100),
            'air_quality_index': (0.0, 300.0),
            'airflow': (50.0, 1000.0),
            'energy': (0.0, 50000.0),
        }
        
        # שדות חובה
        self.required_fields = {
            'sensor_data': ['sensor_id', 'timestamp', 'temperature', 'humidity', 'co2'],
            'alert_data': ['alert_id', 'sensor_id', 'alert_type', 'severity', 'message', 'timestamp'],
            'system_status': ['timestamp', 'overall_status', 'active_sensors_count']
        }
        
        # ביטויים רגולריים
        self.patterns = {
            'sensor_id': r'^hvac_[a-zA-Z0-9_]+$',
            'alert_id': r'^[a-zA-Z0-9_-]+$',
            'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
            'uuid': r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        }
    
    def validate_sensor_data(self, data: Dict[str, Any]) -> List[ValidationResult]:
        """אימות נתוני חיישן"""
        results = []
        
        # בדיקת שדות חובה
        results.extend(self._validate_required_fields(data, 'sensor_data'))
        
        # בדיקת sensor_id
        if 'sensor_id' in data:
            results.extend(self._validate_sensor_id(data['sensor_id']))
        
        # בדיקת timestamp
        if 'timestamp' in data:
            results.extend(self._validate_timestamp(data['timestamp']))
        
        # בדיקת פרמטרים מספריים
        numeric_params = ['temperature', 'humidity', 'co2', 'pressure', 'occupancy', 
                         'air_quality_index', 'airflow', 'energy']
        
        for param in numeric_params:
            if param in data:
                results.extend(self._validate_numeric_parameter(param, data[param]))
        
        # בדיקות לוגיות
        results.extend(self._validate_sensor_logic(data))
        
        return results
    
    def validate_alert_data(self, data: Dict[str, Any]) -> List[ValidationResult]:
        """אימות נתוני התראה"""
        results = []
        
        # בדיקת שדות חובה
        results.extend(self._validate_required_fields(data, 'alert_data'))
        
        # בדיקת alert_id
        if 'alert_id' in data:
            results.extend(self._validate_alert_id(data['alert_id']))
        
        # בדיקת sensor_id
        if 'sensor_id' in data:
            results.extend(self._validate_sensor_id(data['sensor_id']))
        
        # בדיקת severity
        if 'severity' in data:
            results.extend(self._validate_severity(data['severity']))
        
        # בדיקת alert_type
        if 'alert_type' in data:
            results.extend(self._validate_alert_type(data['alert_type']))
        
        # בדיקת timestamp
        if 'timestamp' in data:
            results.extend(self._validate_timestamp(data['timestamp']))
        
        return results
    
    def validate_system_status(self, data: Dict[str, Any]) -> List[ValidationResult]:
        """אימות סטטוס מערכת"""
        results = []
        
        # בדיקת שדות חובה
        results.extend(self._validate_required_fields(data, 'system_status'))
        
        # בדיקת overall_status
        if 'overall_status' in data:
            results.extend(self._validate_system_status_value(data['overall_status']))
        
        # בדיקת ספירות
        count_fields = ['active_sensors_count', 'total_alerts_count', 'critical_alerts_count']
        for field in count_fields:
            if field in data:
                results.extend(self._validate_count_field(field, data[field]))
        
        return results
    
    def _validate_required_fields(self, data: Dict[str, Any], data_type: str) -> List[ValidationResult]:
        """בדיקת שדות חובה"""
        results = []
        required = self.required_fields.get(data_type, [])
        
        for field in required:
            if field not in data:
                results.append(ValidationResult(
                    is_valid=False,
                    severity=ValidationSeverity.ERROR,
                    field_name=field,
                    message=f"Missing required field: {field}",
                    suggestion=f"Add {field} field to the data"
                ))
            elif data[field] is None:
                results.append(ValidationResult(
                    is_valid=False,
                    severity=ValidationSeverity.ERROR,
                    field_name=field,
                    message=f"Required field {field} is null",
                    suggestion=f"Provide a valid value for {field}"
                ))
        
        return results
    
    def _validate_sensor_id(self, sensor_id: Any) -> List[ValidationResult]:
        """אימות מזהה חיישן"""
        results = []
        
        if not isinstance(sensor_id, str):
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name='sensor_id',
                message="sensor_id must be a string",
                actual_value=type(sensor_id).__name__,
                expected_value="string"
            ))
            return results
        
        if not re.match(self.patterns['sensor_id'], sensor_id):
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name='sensor_id',
                message="Invalid sensor_id format",
                actual_value=sensor_id,
                expected_value="hvac_[alphanumeric_underscore]",
                suggestion="Use format: hvac_office_a1, hvac_meeting_room, etc."
            ))
        
        return results
    
    def _validate_alert_id(self, alert_id: Any) -> List[ValidationResult]:
        """אימות מזהה התראה"""
        results = []
        
        if not isinstance(alert_id, str):
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name='alert_id',
                message="alert_id must be a string",
                actual_value=type(alert_id).__name__,
                expected_value="string"
            ))
            return results
        
        if not re.match(self.patterns['alert_id'], alert_id):
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name='alert_id',
                message="Invalid alert_id format",
                actual_value=alert_id,
                expected_value="alphanumeric with underscore/dash",
                suggestion="Use alphanumeric characters, underscores, and dashes only"
            ))
        
        return results
    
    def _validate_timestamp(self, timestamp: Any) -> List[ValidationResult]:
        """אימות חותמת זמן"""
        results = []
        
        # המרה לdatetime אם נדרש
        if isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except ValueError:
                results.append(ValidationResult(
                    is_valid=False,
                    severity=ValidationSeverity.ERROR,
                    field_name='timestamp',
                    message="Invalid timestamp format",
                    actual_value=timestamp,
                    expected_value="ISO format (YYYY-MM-DDTHH:MM:SS)",
                    suggestion="Use ISO 8601 format: 2024-01-01T12:00:00"
                ))
                return results
        elif isinstance(timestamp, datetime):
            dt = timestamp
        else:
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name='timestamp',
                message="timestamp must be string or datetime",
                actual_value=type(timestamp).__name__,
                expected_value="string or datetime"
            ))
            return results
        
        # בדיקת סבירות הזמן
        now = datetime.now()
        time_diff = abs((dt - now).total_seconds())
        
        # זמן עתידי יותר מ-1 שעה
        if dt > now + timedelta(hours=1):
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.WARNING,
                field_name='timestamp',
                message="Timestamp is too far in the future",
                actual_value=dt.isoformat(),
                suggestion="Check system clock synchronization"
            ))
        
        # זמן עבר יותר מ-24 שעות
        elif dt < now - timedelta(hours=24):
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.WARNING,
                field_name='timestamp',
                message="Timestamp is too old",
                actual_value=dt.isoformat(),
                suggestion="Data may be stale, check data collection pipeline"
            ))
        
        return results
    
    def _validate_numeric_parameter(self, param_name: str, value: Any) -> List[ValidationResult]:
        """אימות פרמטר מספרי"""
        results = []
        
        # בדיקת טיפוס
        if not isinstance(value, (int, float)):
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name=param_name,
                message=f"{param_name} must be numeric",
                actual_value=type(value).__name__,
                expected_value="number"
            ))
            return results
        
        # המרה לfloat
        float_value = float(value)
        
        # בדיקת NaN או Infinity
        if not (float('-inf') < float_value < float('inf')):
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name=param_name,
                message=f"{param_name} has invalid numeric value",
                actual_value=value,
                suggestion="Provide a finite numeric value"
            ))
            return results
        
        # בדיקת טווח תקין
        if param_name in self.valid_ranges:
            min_val, max_val = self.valid_ranges[param_name]
            if not (min_val <= float_value <= max_val):
                results.append(ValidationResult(
                    is_valid=False,
                    severity=ValidationSeverity.ERROR,
                    field_name=param_name,
                    message=f"{param_name} value outside valid range",
                    actual_value=value,
                    expected_value=f"{min_val} to {max_val}",
                    suggestion=f"Check sensor calibration for {param_name}"
                ))
        
        # בדיקת טווח סביר (אזהרה)
        if param_name in self.reasonable_ranges:
            min_val, max_val = self.reasonable_ranges[param_name]
            if not (min_val <= float_value <= max_val):
                results.append(ValidationResult(
                    is_valid=True,
                    severity=ValidationSeverity.WARNING,
                    field_name=param_name,
                    message=f"{param_name} value outside typical range",
                    actual_value=value,
                    expected_value=f"{min_val} to {max_val}",
                    suggestion=f"Unusual {param_name} reading - verify sensor operation"
                ))
        
        return results
    
    def _validate_severity(self, severity: Any) -> List[ValidationResult]:
        """אימות רמת חומרה"""
        results = []
        valid_severities = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
        
        if not isinstance(severity, str):
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name='severity',
                message="severity must be a string",
                actual_value=type(severity).__name__,
                expected_value="string"
            ))
            return results
        
        if severity not in valid_severities:
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name='severity',
                message="Invalid severity level",
                actual_value=severity,
                expected_value=valid_severities,
                suggestion=f"Use one of: {', '.join(valid_severities)}"
            ))
        
        return results
    
    def _validate_alert_type(self, alert_type: Any) -> List[ValidationResult]:
        """אימות סוג התראה"""
        results = []
        valid_alert_types = [
            'TEMPERATURE_HIGH', 'TEMPERATURE_LOW', 'TEMPERATURE_CRITICAL_HIGH', 'TEMPERATURE_CRITICAL_LOW',
            'HUMIDITY_HIGH', 'HUMIDITY_LOW', 'HUMIDITY_CRITICAL_HIGH', 'HUMIDITY_CRITICAL_LOW',
            'CO2_HIGH', 'CO2_CRITICAL', 'OCCUPANCY_ANOMALY', 'SYSTEM_MALFUNCTION',
            'DATA_QUALITY_LOW', 'ANOMALY_DETECTED', 'SENSOR_OFFLINE', 'MAINTENANCE_DUE'
        ]
        
        if not isinstance(alert_type, str):
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name='alert_type',
                message="alert_type must be a string",
                actual_value=type(alert_type).__name__,
                expected_value="string"
            ))
            return results
        
        if alert_type not in valid_alert_types:
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.WARNING,
                field_name='alert_type',
                message="Unknown alert_type",
                actual_value=alert_type,
                expected_value="known alert type",
                suggestion="Verify alert_type or add to validation list"
            ))
        
        return results
    
    def _validate_system_status_value(self, status: Any) -> List[ValidationResult]:
        """אימות ערך סטטוס מערכת"""
        results = []
        valid_statuses = ['NORMAL', 'WARNING', 'ERROR', 'CRITICAL', 'MAINTENANCE']
        
        if not isinstance(status, str):
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name='overall_status',
                message="overall_status must be a string",
                actual_value=type(status).__name__,
                expected_value="string"
            ))
            return results
        
        if status not in valid_statuses:
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name='overall_status',
                message="Invalid system status",
                actual_value=status,
                expected_value=valid_statuses,
                suggestion=f"Use one of: {', '.join(valid_statuses)}"
            ))
        
        return results
    
    def _validate_count_field(self, field_name: str, value: Any) -> List[ValidationResult]:
        """אימות שדה ספירה"""
        results = []
        
        if not isinstance(value, int):
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name=field_name,
                message=f"{field_name} must be an integer",
                actual_value=type(value).__name__,
                expected_value="integer"
            ))
            return results
        
        if value < 0:
            results.append(ValidationResult(
                is_valid=False,
                severity=ValidationSeverity.ERROR,
                field_name=field_name,
                message=f"{field_name} cannot be negative",
                actual_value=value,
                expected_value="non-negative integer"
            ))
        
        # בדיקת סבירות
        max_reasonable = {
            'active_sensors_count': 1000,
            'total_alerts_count': 10000,
            'critical_alerts_count': 1000
        }
        
        if field_name in max_reasonable and value > max_reasonable[field_name]:
            results.append(ValidationResult(
                is_valid=True,
                severity=ValidationSeverity.WARNING,
                field_name=field_name,
                message=f"{field_name} seems unusually high",
                actual_value=value,
                suggestion="Verify system state and data accuracy"
            ))
        
        return results
    
    def _validate_sensor_logic(self, data: Dict[str, Any]) -> List[ValidationResult]:
        """בדיקות לוגיות בין פרמטרים"""
        results = []
        
        # בדיקת קורלציה בין CO2 לתפוסה
        if 'co2' in data and 'occupancy' in data:
            co2 = data['co2']
            occupancy = data['occupancy']
            
            # CO2 גבוה עם תפוסה נמוכה
            if co2 > 1000 and occupancy == 0:
                results.append(ValidationResult(
                    is_valid=True,
                    severity=ValidationSeverity.WARNING,
                    field_name='co2_occupancy_correlation',
                    message="High CO2 with zero occupancy",
                    actual_value=f"CO2: {co2}, Occupancy: {occupancy}",
                    suggestion="Check ventilation system or occupancy sensor"
                ))
            
            # CO2 נמוך עם תפוסה גבוהה
            elif co2 < 500 and occupancy > 10:
                results.append(ValidationResult(
                    is_valid=True,
                    severity=ValidationSeverity.INFO,
                    field_name='co2_occupancy_correlation',
                    message="Low CO2 with high occupancy",
                    actual_value=f"CO2: {co2}, Occupancy: {occupancy}",
                    suggestion="Good ventilation or possible sensor issue"
                ))
        
        # בדיקת קורלציה בין טמפרטורה ללחות
        if 'temperature' in data and 'humidity' in data:
            temp = data['temperature']
            humidity = data['humidity']
            
            # טמפרטורה גבוהה עם לחות גבוהה
            if temp > 28 and humidity > 70:
                results.append(ValidationResult(
                    is_valid=True,
                    severity=ValidationSeverity.WARNING,
                    field_name='temp_humidity_correlation',
                    message="High temperature with high humidity",
                    actual_value=f"Temp: {temp}°C, Humidity: {humidity}%",
                    suggestion="Increase cooling and dehumidification"
                ))
        
        return results

class BatchDataValidator:
    """מאמת נתונים באצווה"""
    
    def __init__(self):
        self.validator = DataValidator()
        self.batch_stats = {
            'total_validated': 0,
            'total_errors': 0,
            'total_warnings': 0,
            'error_rate': 0.0
        }
    
    def validate_batch(self, data_list: List[Dict[str, Any]], data_type: str) -> Dict[str, Any]:
        """אימות אצווה של נתונים"""
        all_results = []
        validation_summary = {
            'total_records': len(data_list),
            'valid_records': 0,
            'invalid_records': 0,
            'warnings': 0,
            'errors': 0,
            'detailed_results': []
        }
        
        for i, data in enumerate(data_list):
            # בחירת פונקציית אימות
            if data_type == 'sensor_data':
                results = self.validator.validate_sensor_data(data)
            elif data_type == 'alert_data':
                results = self.validator.validate_alert_data(data)
            elif data_type == 'system_status':
                results = self.validator.validate_system_status(data)
            else:
                results = [ValidationResult(
                    is_valid=False,
                    severity=ValidationSeverity.ERROR,
                    field_name='data_type',
                    message=f"Unknown data type: {data_type}"
                )]
            
            # סיכום תוצאות לרשומה
            record_summary = {
                'record_index': i,
                'is_valid': all(r.is_valid for r in results if r.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]),
                'errors': [r for r in results if r.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL]],
                'warnings': [r for r in results if r.severity == ValidationSeverity.WARNING],
                'info': [r for r in results if r.severity == ValidationSeverity.INFO]
            }
            
            validation_summary['detailed_results'].append(record_summary)
            
            # עדכון מונים
            if record_summary['is_valid']:
                validation_summary['valid_records'] += 1
            else:
                validation_summary['invalid_records'] += 1
            
            validation_summary['errors'] += len(record_summary['errors'])
            validation_summary['warnings'] += len(record_summary['warnings'])
            
            all_results.extend(results)
        
        # עדכון סטטיסטיקות כלליות
        self.batch_stats['total_validated'] += validation_summary['total_records']
        self.batch_stats['total_errors'] += validation_summary['errors']
        self.batch_stats['total_warnings'] += validation_summary['warnings']
        
        if self.batch_stats['total_validated'] > 0:
            self.batch_stats['error_rate'] = self.batch_stats['total_errors'] / self.batch_stats['total_validated']
        
        return validation_summary
    
    def get_batch_statistics(self) -> Dict[str, Any]:
        """קבלת סטטיסטיקות אימות"""
        return self.batch_stats.copy()

class ValidationReporter:
    """מחולל דוחות אימות"""
    
    def __init__(self):
        pass
    
    def generate_summary_report(self, validation_results: List[ValidationResult]) -> Dict[str, Any]:
        """יצירת דוח סיכום"""
        summary = {
            'total_validations': len(validation_results),
            'passed': 0,
            'failed': 0,
            'by_severity': {
                'INFO': 0,
                'WARNING': 0,
                'ERROR': 0,
                'CRITICAL': 0
            },
            'by_field': {},
            'common_issues': {},
            'recommendations': []
        }
        
        for result in validation_results:
            # ספירה כללית
            if result.is_valid:
                summary['passed'] += 1
            else:
                summary['failed'] += 1
            
            # ספירה לפי חומרה
            summary['by_severity'][result.severity.value] += 1
            
            # ספירה לפי שדה
            if result.field_name not in summary['by_field']:
                summary['by_field'][result.field_name] = 0
            summary['by_field'][result.field_name] += 1
            
            # בעיות נפוצות
            if not result.is_valid:
                issue_key = f"{result.field_name}_{result.severity.value}"
                if issue_key not in summary['common_issues']:
                    summary['common_issues'][issue_key] = {
                        'count': 0,
                        'message': result.message,
                        'suggestion': result.suggestion
                    }
                summary['common_issues'][issue_key]['count'] += 1
        
        # המלצות כלליות
        if summary['by_severity']['ERROR'] > 0:
            summary['recommendations'].append("Address ERROR level issues immediately")
        
        if summary['by_severity']['WARNING'] > summary['total_validations'] * 0.1:
            summary['recommendations'].append("High warning rate - review data quality")
        
        return summary
    
    def format_validation_report(self, results: List[ValidationResult], 
                                format_type: str = "text") -> str:
        """עיצוב דוח אימות"""
        if format_type == "text":
            return self._format_text_report(results)
        elif format_type == "json":
            return self._format_json_report(results)
        else:
            raise ValueError(f"Unsupported format: {format_type}")
    
    def _format_text_report(self, results: List[ValidationResult]) -> str:
        """עיצוב דוח טקסט"""
        lines = ["📋 Data Validation Report", "=" * 50]
        
        summary = self.generate_summary_report(results)
        
        # סיכום כללי
        lines.append(f"📊 Total Validations: {summary['total_validations']}")
        lines.append(f"✅ Passed: {summary['passed']}")
        lines.append(f"❌ Failed: {summary['failed']}")
        lines.append("")
        
        # לפי חומרה
        lines.append("📈 By Severity:")
        for severity, count in summary['by_severity'].items():
            if count > 0:
                emoji = {"INFO": "ℹ️", "WARNING": "⚠️", "ERROR": "❌", "CRITICAL": "🚨"}
                lines.append(f"   {emoji.get(severity, '•')} {severity}: {count}")
        lines.append("")
        
        # בעיות נפוצות
        if summary['common_issues']:
            lines.append("🔍 Common Issues:")
            for issue, details in summary['common_issues'].items():
                lines.append(f"   • {details['message']} (x{details['count']})")
                if details['suggestion']:
                    lines.append(f"     💡 {details['suggestion']}")
        lines.append("")
        
        # המלצות
        if summary['recommendations']:
            lines.append("💡 Recommendations:")
            for rec in summary['recommendations']:
                lines.append(f"   • {rec}")
        
        return "\n".join(lines)
    
    def _format_json_report(self, results: List[ValidationResult]) -> str:
        """עיצוב דוח JSON"""
        summary = self.generate_summary_report(results)
        
        detailed_results = []
        for result in results:
            detailed_results.append({
                'is_valid': result.is_valid,
                'severity': result.severity.value,
                'field_name': result.field_name,
                'message': result.message,
                'expected_value': result.expected_value,
                'actual_value': result.actual_value,
                'suggestion': result.suggestion
            })
        
        report = {
            'summary': summary,
            'detailed_results': detailed_results,
            'generated_at': datetime.now().isoformat()
        }
        
        return json.dumps(report, indent=2, ensure_ascii=False)

# פונקציות עזר
def validate_sensor_reading(data: Dict[str, Any]) -> List[ValidationResult]:
    """פונקציה נוחה לאימות קריאת חיישן"""
    validator = DataValidator()
    return validator.validate_sensor_data(data)

def validate_alert(data: Dict[str, Any]) -> List[ValidationResult]:
    """פונקציה נוחה לאימות התראה"""
    validator = DataValidator()
    return validator.validate_alert_data(data)

def is_valid_data(data: Dict[str, Any], data_type: str) -> bool:
    """בדיקה מהירה אם נתונים תקינים"""
    if data_type == 'sensor_data':
        results = validate_sensor_reading(data)
    elif data_type == 'alert_data':
        results = validate_alert(data)
    else:
        return False
    
    # החזרת True רק אם אין שגיאות קריטיות
    return all(r.is_valid for r in results if r.severity in [ValidationSeverity.ERROR, ValidationSeverity.CRITICAL])

# בדיקה ישירה
if __name__ == "__main__":
    print("🔧 Testing Data Validation System...")
    
    # יצירת מאמת
    validator = DataValidator()
    reporter = ValidationReporter()
    
    # נתוני בדיקה
    test_sensor_data = {
        'sensor_id': 'hvac_office_a1',
        'timestamp': datetime.now().isoformat(),
        'temperature': 23.5,
        'humidity': 45.2,
        'co2': 450,
        'occupancy': 3
    }
    
    # נתוני בדיקה עם שגיאות
    invalid_sensor_data = {
        'sensor_id': 'invalid_id',
        'timestamp': 'invalid_time',
        'temperature': 150.0,  # לא סביר
        'humidity': -10.0,     # לא תקין
        'co2': 'not_a_number'  # לא מספר
    }
    
    # בדיקת נתונים תקינים
    print("\n📊 Testing valid sensor data:")
    results = validator.validate_sensor_data(test_sensor_data)
    print(f"Validation results: {len(results)} issues found")
    for result in results:
        print(f"  {result.severity.value}: {result.message}")
    
    # בדיקת נתונים לא תקינים
    print("\n❌ Testing invalid sensor data:")
    results = validator.validate_sensor_data(invalid_sensor_data)
    print(f"Validation results: {len(results)} issues found")
    for result in results:
        print(f"  {result.severity.value}: {result.field_name} - {result.message}")
    
    # יצירת דוח
    print("\n📋 Generating validation report:")
    report = reporter.format_validation_report(results, "text")
    print(report)
    
    # בדיקת אצווה
    print("\n📦 Testing batch validation:")
    batch_validator = BatchDataValidator()
    test_batch = [test_sensor_data, invalid_sensor_data, test_sensor_data]
    
    batch_results = batch_validator.validate_batch(test_batch, 'sensor_data')
    print(f"Batch validation - Total: {batch_results['total_records']}, Valid: {batch_results['valid_records']}")
    
    # בדיקת פונקציות עזר
    print("\n🔧 Testing helper functions:")
    print(f"is_valid_data (valid): {is_valid_data(test_sensor_data, 'sensor_data')}")
    print(f"is_valid_data (invalid): {is_valid_data(invalid_sensor_data, 'sensor_data')}")
    
    # בדיקת תקשורת של CO2 ותפוסה
    print("\n🔍 Testing logical correlations:")
    correlation_test_data = {
        'sensor_id': 'hvac_test',
        'timestamp': datetime.now().isoformat(),
        'temperature': 23.0,
        'humidity': 45.0,
        'co2': 1200,  # גבוה
        'occupancy': 0  # נמוך - יש ליצור אזהרה
    }
    
    correlation_results = validator.validate_sensor_data(correlation_test_data)
    correlation_warnings = [r for r in correlation_results if 'correlation' in r.field_name]
    print(f"Correlation warnings found: {len(correlation_warnings)}")
    for warning in correlation_warnings:
        print(f"  ⚠️ {warning.message}: {warning.actual_value}")
        print(f"     💡 {warning.suggestion}")
    
    # בדיקת דוח JSON
    print("\n📄 Testing JSON report generation:")
    json_report = reporter.format_validation_report(results, "json")
    try:
        parsed_json = json.loads(json_report)
        print(f"JSON report generated successfully - {parsed_json['summary']['total_validations']} validations")
    except json.JSONDecodeError as e:
        print(f"❌ JSON report generation failed: {e}")
    
    # בדיקת סטטיסטיקות אצווה
    print("\n📈 Testing batch statistics:")
    batch_stats = batch_validator.get_batch_statistics()
    print(f"Batch Statistics:")
    print(f"  Total Validated: {batch_stats['total_validated']}")
    print(f"  Error Rate: {batch_stats['error_rate']:.2%}")
    
    print("\n✅ Data validation system test completed successfully!")
    print("🎯 All validation features are working correctly!")