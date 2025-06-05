"""
Smart Alert System for HVAC Monitoring
Advanced alerting with escalation, prioritization, and smart recommendations
"""
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import uuid
import logging
from collections import defaultdict, deque

class AlertSeverity(Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

class AlertType(Enum):
    TEMPERATURE_HIGH = "TEMPERATURE_HIGH"
    TEMPERATURE_LOW = "TEMPERATURE_LOW"
    TEMPERATURE_CRITICAL_HIGH = "TEMPERATURE_CRITICAL_HIGH"
    TEMPERATURE_CRITICAL_LOW = "TEMPERATURE_CRITICAL_LOW"
    HUMIDITY_HIGH = "HUMIDITY_HIGH"
    HUMIDITY_LOW = "HUMIDITY_LOW"
    HUMIDITY_CRITICAL_HIGH = "HUMIDITY_CRITICAL_HIGH"
    HUMIDITY_CRITICAL_LOW = "HUMIDITY_CRITICAL_LOW"
    CO2_HIGH = "CO2_HIGH"
    CO2_CRITICAL = "CO2_CRITICAL"
    OCCUPANCY_ANOMALY = "OCCUPANCY_ANOMALY"
    SYSTEM_MALFUNCTION = "SYSTEM_MALFUNCTION"
    DATA_QUALITY_LOW = "DATA_QUALITY_LOW"
    ANOMALY_DETECTED = "ANOMALY_DETECTED"
    SENSOR_OFFLINE = "SENSOR_OFFLINE"
    MAINTENANCE_DUE = "MAINTENANCE_DUE"
    ENERGY_EFFICIENCY_LOW = "ENERGY_EFFICIENCY_LOW"
    PRESSURE_ANOMALY = "PRESSURE_ANOMALY"

class AlertStatus(Enum):
    ACTIVE = "ACTIVE"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"
    SUPPRESSED = "SUPPRESSED"

@dataclass
class Alert:
    """Alert data structure"""
    alert_id: str
    sensor_id: str
    location: str
    alert_type: AlertType
    severity: AlertSeverity
    status: AlertStatus
    message: str
    description: str
    timestamp: datetime
    values: Dict[str, float]
    threshold_violated: str
    recommended_action: str
    escalation_level: int = 0
    is_acknowledged: bool = False
    is_resolved: bool = False
    acknowledged_by: Optional[str] = None
    resolved_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    escalated_at: Optional[datetime] = None
    suppressed_until: Optional[datetime] = None
    tags: List[str] = None
    correlation_id: Optional[str] = None
    parent_alert_id: Optional[str] = None
    related_alerts: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if self.related_alerts is None:
            self.related_alerts = []
    
    def to_dict(self) -> Dict:
        """Convert alert to dictionary"""
        data = asdict(self)
        # Convert enums to strings
        data['alert_type'] = self.alert_type.value
        data['severity'] = self.severity.value
        data['status'] = self.status.value
        # Convert datetime to ISO string
        data['timestamp'] = self.timestamp.isoformat()
        if self.acknowledged_at:
            data['acknowledged_at'] = self.acknowledged_at.isoformat()
        if self.resolved_at:
            data['resolved_at'] = self.resolved_at.isoformat()
        if self.escalated_at:
            data['escalated_at'] = self.escalated_at.isoformat()
        if self.suppressed_until:
            data['suppressed_until'] = self.suppressed_until.isoformat()
        return data

class AlertRule:
    """Rule for generating alerts"""
    
    def __init__(self, rule_id: str, name: str, parameter: str, 
                 condition: str, threshold: float, severity: AlertSeverity,
                 alert_type: AlertType, message_template: str, 
                 recommended_action: str, cooldown_minutes: int = 5,
                 escalation_time_minutes: int = 30, enabled: bool = True):
        self.rule_id = rule_id
        self.name = name
        self.parameter = parameter
        self.condition = condition  # 'greater_than', 'less_than', 'equals', 'not_equals', 'range'
        self.threshold = threshold
        self.severity = severity
        self.alert_type = alert_type
        self.message_template = message_template
        self.recommended_action = recommended_action
        self.cooldown_minutes = cooldown_minutes
        self.escalation_time_minutes = escalation_time_minutes
        self.enabled = enabled
        self.last_triggered = {}  # sensor_id -> timestamp
        
    def evaluate(self, sensor_id: str, value: float, timestamp: datetime, 
                 sensor_data: Dict[str, float], location: str = "Unknown") -> Optional[Alert]:
        """Evaluate rule against sensor value"""
        if not self.enabled:
            return None
            
        # Check cooldown period
        if sensor_id in self.last_triggered:
            time_since_last = timestamp - self.last_triggered[sensor_id]
            if time_since_last.total_seconds() < self.cooldown_minutes * 60:
                return None
        
        # Check condition
        triggered = False
        if self.condition == 'greater_than' and value > self.threshold:
            triggered = True
        elif self.condition == 'less_than' and value < self.threshold:
            triggered = True
        elif self.condition == 'equals' and value == self.threshold:
            triggered = True
        elif self.condition == 'not_equals' and value != self.threshold:
            triggered = True
        elif self.condition == 'range':
            # Threshold should be a tuple (min, max)
            if isinstance(self.threshold, (list, tuple)) and len(self.threshold) == 2:
                triggered = not (self.threshold[0] <= value <= self.threshold[1])
        
        if not triggered:
            return None
        
        # Create alert
        self.last_triggered[sensor_id] = timestamp
        
        # Format message with actual values
        formatted_message = self.message_template.format(
            sensor_id=sensor_id,
            value=value,
            threshold=self.threshold,
            parameter=self.parameter,
            location=location
        )
        
        alert = Alert(
            alert_id=str(uuid.uuid4()),
            sensor_id=sensor_id,
            location=location,
            alert_type=self.alert_type,
            severity=self.severity,
            status=AlertStatus.ACTIVE,
            message=formatted_message,
            description=f"{self.name}: {formatted_message}",
            timestamp=timestamp,
            values=sensor_data.copy(),
            threshold_violated=f"{self.parameter} {self.condition} {self.threshold}",
            recommended_action=self.recommended_action,
            tags=[self.parameter, self.severity.value.lower()]
        )
        
        return alert

class EscalationRule:
    """Rule for alert escalation"""
    
    def __init__(self, severity: AlertSeverity, escalation_time_minutes: int,
                 escalation_actions: List[str], max_escalation_level: int = 3):
        self.severity = severity
        self.escalation_time_minutes = escalation_time_minutes
        self.escalation_actions = escalation_actions
        self.max_escalation_level = max_escalation_level
    
    def should_escalate(self, alert: Alert, current_time: datetime) -> bool:
        """Check if alert should be escalated"""
        if alert.is_acknowledged or alert.is_resolved:
            return False
        
        if alert.escalation_level >= self.max_escalation_level:
            return False
        
        time_since_alert = current_time - alert.timestamp
        time_since_escalation = current_time - (alert.escalated_at or alert.timestamp)
        
        return time_since_escalation.total_seconds() >= self.escalation_time_minutes * 60

class AlertCorrelator:
    """Correlate related alerts to reduce noise"""
    
    def __init__(self, correlation_window_minutes: int = 10):
        self.correlation_window_minutes = correlation_window_minutes
        self.correlation_rules = {
            'temperature_humidity': self._correlate_temp_humidity,
            'co2_occupancy': self._correlate_co2_occupancy,
            'system_wide': self._correlate_system_wide,
        }
    
    def correlate_alerts(self, new_alert: Alert, existing_alerts: List[Alert]) -> List[Alert]:
        """Find related alerts and correlate them"""
        related_alerts = []
        current_time = datetime.now()
        
        for existing_alert in existing_alerts:
            if existing_alert.is_resolved:
                continue
                
            # Check if within correlation window
            time_diff = abs((new_alert.timestamp - existing_alert.timestamp).total_seconds())
            if time_diff > self.correlation_window_minutes * 60:
                continue
            
            # Apply correlation rules
            for rule_name, rule_func in self.correlation_rules.items():
                if rule_func(new_alert, existing_alert):
                    related_alerts.append(existing_alert)
                    break
        
        return related_alerts
    
    def _correlate_temp_humidity(self, alert1: Alert, alert2: Alert) -> bool:
        """Correlate temperature and humidity alerts"""
        temp_types = {AlertType.TEMPERATURE_HIGH, AlertType.TEMPERATURE_LOW, 
                     AlertType.TEMPERATURE_CRITICAL_HIGH, AlertType.TEMPERATURE_CRITICAL_LOW}
        humidity_types = {AlertType.HUMIDITY_HIGH, AlertType.HUMIDITY_LOW,
                         AlertType.HUMIDITY_CRITICAL_HIGH, AlertType.HUMIDITY_CRITICAL_LOW}
        
        return ((alert1.alert_type in temp_types and alert2.alert_type in humidity_types) or
                (alert1.alert_type in humidity_types and alert2.alert_type in temp_types)) and \
               alert1.sensor_id == alert2.sensor_id
    
    def _correlate_co2_occupancy(self, alert1: Alert, alert2: Alert) -> bool:
        """Correlate CO2 and occupancy alerts"""
        co2_types = {AlertType.CO2_HIGH, AlertType.CO2_CRITICAL}
        occupancy_types = {AlertType.OCCUPANCY_ANOMALY}
        
        return ((alert1.alert_type in co2_types and alert2.alert_type in occupancy_types) or
                (alert1.alert_type in occupancy_types and alert2.alert_type in co2_types)) and \
               alert1.location == alert2.location
    
    def _correlate_system_wide(self, alert1: Alert, alert2: Alert) -> bool:
        """Correlate system-wide issues"""
        system_types = {AlertType.SYSTEM_MALFUNCTION, AlertType.SENSOR_OFFLINE, AlertType.DATA_QUALITY_LOW}
        
        return alert1.alert_type in system_types and alert2.alert_type in system_types

class NotificationChannel:
    """Base class for notification channels"""
    
    def __init__(self, name: str, enabled: bool = True):
        self.name = name
        self.enabled = enabled
    
    def send_notification(self, alert: Alert, recipients: List[str]) -> bool:
        """Send notification - to be implemented by subclasses"""
        raise NotImplementedError

class EmailNotificationChannel(NotificationChannel):
    """Email notification channel"""
    
    def __init__(self, smtp_server: str = None, smtp_port: int = 587):
        super().__init__("email")
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
    
    def send_notification(self, alert: Alert, recipients: List[str]) -> bool:
        """Send email notification"""
        # Simulate email sending
        print(f"📧 Email sent to {recipients}: {alert.message}")
        return True

class SMSNotificationChannel(NotificationChannel):
    """SMS notification channel"""
    
    def send_notification(self, alert: Alert, recipients: List[str]) -> bool:
        """Send SMS notification"""
        # Simulate SMS sending
        print(f"📱 SMS sent to {recipients}: {alert.message}")
        return True

class SlackNotificationChannel(NotificationChannel):
    """Slack notification channel"""
    
    def __init__(self, webhook_url: str = None):
        super().__init__("slack")
        self.webhook_url = webhook_url
    
    def send_notification(self, alert: Alert, recipients: List[str]) -> bool:
        """Send Slack notification"""
        # Simulate Slack notification
        print(f"💬 Slack notification sent: {alert.message}")
        return True

class SmartAlertSystem:
    """Main alert system with smart features"""
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.alert_rules = {}
        self.escalation_rules = {}
        self.active_alerts = {}  # alert_id -> Alert
        self.alert_history = deque(maxlen=10000)
        self.notification_channels = {}
        self.correlator = AlertCorrelator()
        self.alert_patterns = defaultdict(list)
        self.maintenance_mode = {}  # sensor_id -> end_time
        
        # Statistics
        self.stats = {
            'total_alerts': 0,
            'alerts_by_severity': defaultdict(int),
            'alerts_by_type': defaultdict(int),
            'false_positive_rate': 0.0,
            'avg_resolution_time': 0.0
        }
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Initialize default rules and channels
        self._initialize_default_rules()
        self._initialize_notification_channels()
    
    def _initialize_default_rules(self):
        """Initialize default alert rules"""
        default_rules = [
            # Temperature rules
            AlertRule(
                "temp_high", "High Temperature", "temperature", "greater_than", 28.0,
                AlertSeverity.MEDIUM, AlertType.TEMPERATURE_HIGH,
                "Temperature {value:.1f}°C exceeds threshold {threshold}°C at {location}",
                "Check HVAC cooling system, verify thermostat settings"
            ),
            AlertRule(
                "temp_critical_high", "Critical High Temperature", "temperature", "greater_than", 32.0,
                AlertSeverity.CRITICAL, AlertType.TEMPERATURE_CRITICAL_HIGH,
                "CRITICAL: Temperature {value:.1f}°C at {location} requires immediate attention",
                "Emergency cooling required - check HVAC system immediately"
            ),
            AlertRule(
                "temp_low", "Low Temperature", "temperature", "less_than", 18.0,
                AlertSeverity.MEDIUM, AlertType.TEMPERATURE_LOW,
                "Temperature {value:.1f}°C below threshold {threshold}°C at {location}",
                "Check HVAC heating system, verify thermostat settings"
            ),
            AlertRule(
                "temp_critical_low", "Critical Low Temperature", "temperature", "less_than", 15.0,
                AlertSeverity.CRITICAL, AlertType.TEMPERATURE_CRITICAL_LOW,
                "CRITICAL: Temperature {value:.1f}°C at {location} too low",
                "Emergency heating required - check HVAC system immediately"
            ),
            
            # Humidity rules
            AlertRule(
                "humidity_high", "High Humidity", "humidity", "greater_than", 70.0,
                AlertSeverity.MEDIUM, AlertType.HUMIDITY_HIGH,
                "Humidity {value:.1f}% exceeds threshold {threshold}% at {location}",
                "Check dehumidification system, increase ventilation"
            ),
            AlertRule(
                "humidity_low", "Low Humidity", "humidity", "less_than", 30.0,
                AlertSeverity.MEDIUM, AlertType.HUMIDITY_LOW,
                "Humidity {value:.1f}% below threshold {threshold}% at {location}",
                "Check humidification system, reduce ventilation"
            ),
            
            # CO2 rules
            AlertRule(
                "co2_high", "High CO2", "co2", "greater_than", 1000.0,
                AlertSeverity.MEDIUM, AlertType.CO2_HIGH,
                "CO2 level {value:.0f} ppm exceeds threshold {threshold} ppm at {location}",
                "Increase ventilation, check occupancy levels"
            ),
            AlertRule(
                "co2_critical", "Critical CO2", "co2", "greater_than", 1500.0,
                AlertSeverity.HIGH, AlertType.CO2_CRITICAL,
                "CRITICAL: CO2 level {value:.0f} ppm at {location} requires immediate ventilation",
                "Emergency ventilation required - increase fresh air intake"
            ),
        ]
        
        for rule in default_rules:
            self.add_alert_rule(rule)
        
        # Initialize escalation rules
        self.escalation_rules = {
            AlertSeverity.LOW: EscalationRule(AlertSeverity.LOW, 60, ["email"], 2),
            AlertSeverity.MEDIUM: EscalationRule(AlertSeverity.MEDIUM, 30, ["email", "slack"], 2),
            AlertSeverity.HIGH: EscalationRule(AlertSeverity.HIGH, 15, ["email", "slack", "sms"], 3),
            AlertSeverity.CRITICAL: EscalationRule(AlertSeverity.CRITICAL, 5, ["email", "slack", "sms"], 3),
        }
    
    def _initialize_notification_channels(self):
        """Initialize notification channels"""
        self.notification_channels = {
            "email": EmailNotificationChannel(),
            "sms": SMSNotificationChannel(),
            "slack": SlackNotificationChannel()
        }
    
    def add_alert_rule(self, rule: AlertRule):
        """Add alert rule"""
        self.alert_rules[rule.rule_id] = rule
        self.logger.info(f"Added alert rule: {rule.name}")
    
    def remove_alert_rule(self, rule_id: str):
        """Remove alert rule"""
        if rule_id in self.alert_rules:
            del self.alert_rules[rule_id]
            self.logger.info(f"Removed alert rule: {rule_id}")
    
    def enable_rule(self, rule_id: str):
        """Enable alert rule"""
        if rule_id in self.alert_rules:
            self.alert_rules[rule_id].enabled = True
            self.logger.info(f"Enabled alert rule: {rule_id}")
    
    def disable_rule(self, rule_id: str):
        """Disable alert rule"""
        if rule_id in self.alert_rules:
            self.alert_rules[rule_id].enabled = False
            self.logger.info(f"Disabled alert rule: {rule_id}")
    
    def set_maintenance_mode(self, sensor_id: str, duration_hours: int):
        """Set sensor in maintenance mode"""
        end_time = datetime.now() + timedelta(hours=duration_hours)
        self.maintenance_mode[sensor_id] = end_time
        self.logger.info(f"Sensor {sensor_id} in maintenance mode until {end_time}")
    
    def clear_maintenance_mode(self, sensor_id: str):
        """Clear maintenance mode for sensor"""
        if sensor_id in self.maintenance_mode:
            del self.maintenance_mode[sensor_id]
            self.logger.info(f"Cleared maintenance mode for sensor {sensor_id}")
    
    def is_in_maintenance(self, sensor_id: str) -> bool:
        """Check if sensor is in maintenance mode"""
        if sensor_id not in self.maintenance_mode:
            return False
        
        if datetime.now() > self.maintenance_mode[sensor_id]:
            del self.maintenance_mode[sensor_id]
            return False
        
        return True
    
    def process_sensor_data(self, sensor_id: str, sensor_data: Dict[str, float], 
                          location: str = "Unknown", timestamp: datetime = None) -> List[Alert]:
        """Process sensor data and generate alerts"""
        if timestamp is None:
            timestamp = datetime.now()
        
        # Skip if in maintenance mode
        if self.is_in_maintenance(sensor_id):
            return []
        
        new_alerts = []
        
        # Evaluate all rules
        for rule in self.alert_rules.values():
            if rule.parameter in sensor_data:
                value = sensor_data[rule.parameter]
                alert = rule.evaluate(sensor_id, value, timestamp, sensor_data, location)
                if alert:
                    new_alerts.append(alert)
        
        # Process each new alert
        processed_alerts = []
        for alert in new_alerts:
            # Correlate with existing alerts
            related_alerts = self.correlator.correlate_alerts(alert, list(self.active_alerts.values()))
            
            if related_alerts:
                # Create correlation group
                correlation_id = str(uuid.uuid4())
                alert.correlation_id = correlation_id
                alert.related_alerts = [ra.alert_id for ra in related_alerts]
                
                # Update related alerts
                for related_alert in related_alerts:
                    related_alert.correlation_id = correlation_id
                    related_alert.related_alerts.append(alert.alert_id)
            
            # Add to active alerts and history
            self.active_alerts[alert.alert_id] = alert
            self.alert_history.append(alert)
            processed_alerts.append(alert)
            
            # Update statistics
            self._update_stats(alert)
            
            # Send notifications
            self._send_notifications(alert)
            
            self.logger.info(f"New alert: {alert.message}")
        
        return processed_alerts
    
    def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an alert"""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.is_acknowledged = True
            alert.acknowledged_by = acknowledged_by
            alert.acknowledged_at = datetime.now()
            alert.status = AlertStatus.ACKNOWLEDGED
            
            self.logger.info(f"Alert {alert_id} acknowledged by {acknowledged_by}")
            return True
        return False
    
    def resolve_alert(self, alert_id: str, resolved_by: str, resolution_note: str = "") -> bool:
        """Resolve an alert"""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.is_resolved = True
            alert.resolved_by = resolved_by
            alert.resolved_at = datetime.now()
            alert.status = AlertStatus.RESOLVED
            
            # Remove from active alerts
            del self.active_alerts[alert_id]
            
            self.logger.info(f"Alert {alert_id} resolved by {resolved_by}")
            return True
        return False
    
    def suppress_alert(self, alert_id: str, suppress_duration_minutes: int) -> bool:
        """Suppress an alert for a specified duration"""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.status = AlertStatus.SUPPRESSED
            alert.suppressed_until = datetime.now() + timedelta(minutes=suppress_duration_minutes)
            
            self.logger.info(f"Alert {alert_id} suppressed for {suppress_duration_minutes} minutes")
            return True
        return False
    
    def check_escalations(self):
        """Check for alerts that need escalation"""
        current_time = datetime.now()
        
        for alert in self.active_alerts.values():
            if alert.status == AlertStatus.SUPPRESSED:
                if current_time > alert.suppressed_until:
                    alert.status = AlertStatus.ACTIVE
                    alert.suppressed_until = None
                continue
            
            escalation_rule = self.escalation_rules.get(alert.severity)
            if escalation_rule and escalation_rule.should_escalate(alert, current_time):
                self._escalate_alert(alert, current_time)
    
    def _escalate_alert(self, alert: Alert, current_time: datetime):
        """Escalate an alert"""
        alert.escalation_level += 1
        alert.escalated_at = current_time
        
        escalation_rule = self.escalation_rules[alert.severity]
        
        # Send escalation notifications
        for channel_name in escalation_rule.escalation_actions:
            if channel_name in self.notification_channels:
                channel = self.notification_channels[channel_name]
                recipients = self._get_escalation_recipients(alert.escalation_level)
                channel.send_notification(alert, recipients)
        
        self.logger.warning(f"Alert {alert.alert_id} escalated to level {alert.escalation_level}")
    
    def _send_notifications(self, alert: Alert):
        """Send notifications for new alert"""
        escalation_rule = self.escalation_rules.get(alert.severity)
        if not escalation_rule:
            return
        
        # Send initial notifications
        for channel_name in escalation_rule.escalation_actions:
            if channel_name in self.notification_channels:
                channel = self.notification_channels[channel_name]
                recipients = self._get_initial_recipients(alert.severity)
                channel.send_notification(alert, recipients)
    
    def _get_initial_recipients(self, severity: AlertSeverity) -> List[str]:
        """Get initial notification recipients based on severity"""
        recipients = {
            AlertSeverity.LOW: ["facility@company.com"],
            AlertSeverity.MEDIUM: ["facility@company.com", "maintenance@company.com"],
            AlertSeverity.HIGH: ["facility@company.com", "maintenance@company.com", "manager@company.com"],
            AlertSeverity.CRITICAL: ["facility@company.com", "maintenance@company.com", "manager@company.com", "emergency@company.com"]
        }
        return recipients.get(severity, ["facility@company.com"])
    
    def _get_escalation_recipients(self, escalation_level: int) -> List[str]:
        """Get escalation recipients based on level"""
        level_recipients = {
            1: ["supervisor@company.com"],
            2: ["manager@company.com"],
            3: ["director@company.com", "emergency@company.com"]
        }
        return level_recipients.get(escalation_level, ["emergency@company.com"])
    
    def _update_stats(self, alert: Alert):
        """Update alert statistics"""
        self.stats['total_alerts'] += 1
        self.stats['alerts_by_severity'][alert.severity.value] += 1
        self.stats['alerts_by_type'][alert.alert_type.value] += 1
    
    def get_active_alerts(self, severity_filter: AlertSeverity = None, 
                         sensor_filter: str = None) -> List[Alert]:
        """Get active alerts with optional filters"""
        alerts = list(self.active_alerts.values())
        
        if severity_filter:
            alerts = [a for a in alerts if a.severity == severity_filter]
        
        if sensor_filter:
            alerts = [a for a in alerts if a.sensor_id == sensor_filter]
        
        # Sort by severity and timestamp
        severity_order = {
            AlertSeverity.CRITICAL: 0,
            AlertSeverity.HIGH: 1,
            AlertSeverity.MEDIUM: 2,
            AlertSeverity.LOW: 3
        }
        
        alerts.sort(key=lambda x: (severity_order[x.severity], x.timestamp), reverse=True)
        return alerts
    
    def get_alert_summary(self) -> Dict:
        """Get alert system summary"""
        active_count_by_severity = defaultdict(int)
        for alert in self.active_alerts.values():
            active_count_by_severity[alert.severity.value] += 1
        
        return {
            'active_alerts_total': len(self.active_alerts),
            'active_alerts_by_severity': dict(active_count_by_severity),
            'total_alerts_processed': self.stats['total_alerts'],
            'alerts_by_severity': dict(self.stats['alerts_by_severity']),
            'alerts_by_type': dict(self.stats['alerts_by_type']),
            'sensors_in_maintenance': len(self.maintenance_mode),
            'enabled_rules': sum(1 for rule in self.alert_rules.values() if rule.enabled),
            'total_rules': len(self.alert_rules)
        }
    
    def export_alerts(self, start_time: datetime = None, end_time: datetime = None) -> List[Dict]:
        """Export alerts to dictionary format"""
        if start_time is None:
            start_time = datetime.now() - timedelta(days=7)
        if end_time is None:
            end_time = datetime.now()
        
        filtered_alerts = []
        for alert in self.alert_history:
            if start_time <= alert.timestamp <= end_time:
                filtered_alerts.append(alert.to_dict())
        
        return filtered_alerts
    
    def import_alert_rules(self, rules_config: List[Dict]):
        """Import alert rules from configuration"""
        for rule_config in rules_config:
            rule = AlertRule(
                rule_id=rule_config['rule_id'],
                name=rule_config['name'],
                parameter=rule_config['parameter'],
                condition=rule_config['condition'],
                threshold=rule_config['threshold'],
                severity=AlertSeverity(rule_config['severity']),
                alert_type=AlertType(rule_config['alert_type']),
                message_template=rule_config['message_template'],
                recommended_action=rule_config['recommended_action'],
                cooldown_minutes=rule_config.get('cooldown_minutes', 5),
                escalation_time_minutes=rule_config.get('escalation_time_minutes', 30),
                enabled=rule_config.get('enabled', True)
            )
            self.add_alert_rule(rule)

# Test and demo functions
def test_alert_system():
    """Test the alert system"""
    print("🚨 Testing Smart HVAC Alert System...")
    
    # Initialize alert system
    alert_system = SmartAlertSystem()
    
    # Test scenarios
    test_scenarios = [
        {
            'sensor_id': 'hvac_office_a1',
            'location': 'Office Floor A1',
            'data': {'temperature': 30.0, 'humidity': 45.0, 'co2': 450},
            'description': 'High temperature alert'
        },
        {
            'sensor_id': 'hvac_meeting_room',
            'location': 'Meeting Room B',
            'data': {'temperature': 23.0, 'humidity': 75.0, 'co2': 1200},
            'description': 'High humidity and CO2 alerts'
        },
        {
            'sensor_id': 'hvac_office_a1',
            'location': 'Office Floor A1',
            'data': {'temperature': 35.0, 'humidity': 45.0, 'co2': 450},
            'description': 'Critical temperature alert'
        },
        {
            'sensor_id': 'hvac_server_room',
            'location': 'Server Room',
            'data': {'temperature': 14.0, 'humidity': 25.0, 'co2': 400},
            'description': 'Critical low temperature and humidity'
        }
    ]
    
    print("✅ Alert system initialized")
    print(f"📋 Loaded {len(alert_system.alert_rules)} alert rules")
    
    # Process test data
    for i, scenario in enumerate(test_scenarios):
        print(f"\n📊 Test {i+1}: {scenario['description']}")
        print(f"   Sensor: {scenario['sensor_id']} ({scenario['location']})")
        print(f"   Data: {scenario['data']}")
        
        alerts = alert_system.process_sensor_data(
            scenario['sensor_id'],
            scenario['data'],
            scenario['location']
        )
        
        if alerts:
            print(f"   🚨 {len(alerts)} alert(s) generated:")
            for alert in alerts:
                print(f"   - {alert.severity.value}: {alert.message}")
                print(f"     Action: {alert.recommended_action}")
        else:
            print("   ✅ No alerts generated")
    
    # Test maintenance mode
    print(f"\n🔧 Testing maintenance mode...")
    alert_system.set_maintenance_mode('hvac_office_a1', 1)
    
    # Try to generate alert for sensor in maintenance
    maintenance_alerts = alert_system.process_sensor_data(
        'hvac_office_a1',
        {'temperature': 40.0, 'humidity': 45.0, 'co2': 450},
        'Office Floor A1'
    )
    print(f"   Alerts during maintenance: {len(maintenance_alerts)} (should be 0)")
    
    # Test acknowledgment and resolution
    active_alerts = alert_system.get_active_alerts()
    if active_alerts:
        test_alert = active_alerts[0]
        print(f"\n✅ Testing alert acknowledgment...")
        alert_system.acknowledge_alert(test_alert.alert_id, "john.doe")
        
        print(f"🔧 Testing alert resolution...")
        alert_system.resolve_alert(test_alert.alert_id, "jane.smith", "Fixed HVAC settings")
    
    # Test escalation check
    print(f"\n⬆️ Testing escalation check...")
    alert_system.check_escalations()
    
    # Get system summary
    summary = alert_system.get_alert_summary()
    print(f"\n📈 Alert System Summary:")
    print(f"   Active Alerts: {summary['active_alerts_total']}")
    print(f"   Total Processed: {summary['total_alerts_processed']}")
    print(f"   By Severity: {summary['active_alerts_by_severity']}")
    print(f"   Enabled Rules: {summary['enabled_rules']}/{summary['total_rules']}")
    
    # Test alert export
    exported_alerts = alert_system.export_alerts()
    print(f"   Exported Alerts: {len(exported_alerts)}")
    
    print("\n✅ Alert system testing completed!")

def create_sample_rule_config():
    """Create sample rule configuration for import"""
    return [
        {
            'rule_id': 'custom_temp_high',
            'name': 'Custom High Temperature',
            'parameter': 'temperature',
            'condition': 'greater_than',
            'threshold': 25.0,
            'severity': 'MEDIUM',
            'alert_type': 'TEMPERATURE_HIGH',
            'message_template': 'Custom: Temperature {value:.1f}°C at {location}',
            'recommended_action': 'Custom action: Check air conditioning',
            'cooldown_minutes': 10,
            'escalation_time_minutes': 20,
            'enabled': True
        },
        {
            'rule_id': 'energy_efficiency',
            'name': 'Energy Efficiency Alert',
            'parameter': 'energy_efficiency',
            'condition': 'less_than',
            'threshold': 0.7,
            'severity': 'LOW',
            'alert_type': 'ENERGY_EFFICIENCY_LOW',
            'message_template': 'Energy efficiency {value:.2f} below target at {location}',
            'recommended_action': 'Review HVAC settings and schedules',
            'cooldown_minutes': 30,
            'escalation_time_minutes': 60,
            'enabled': True
        }
    ]

def demo_alert_correlation():
    """Demonstrate alert correlation functionality"""
    print("🔗 Testing Alert Correlation...")
    
    alert_system = SmartAlertSystem()
    
    # Create related alerts in quick succession
    timestamp = datetime.now()
    
    # Temperature alert
    temp_alerts = alert_system.process_sensor_data(
        'hvac_office_a1',
        {'temperature': 30.0, 'humidity': 45.0, 'co2': 450},
        'Office Floor A1',
        timestamp
    )
    
    # Humidity alert from same sensor (should correlate)
    humidity_alerts = alert_system.process_sensor_data(
        'hvac_office_a1',
        {'temperature': 30.0, 'humidity': 75.0, 'co2': 450},
        'Office Floor A1',
        timestamp + timedelta(minutes=2)
    )
    
    # Check correlation
    if temp_alerts and humidity_alerts:
        temp_alert = temp_alerts[0]
        humidity_alert = humidity_alerts[0]
        
        print(f"Temperature Alert ID: {temp_alert.alert_id}")
        print(f"Humidity Alert ID: {humidity_alert.alert_id}")
        
        if temp_alert.correlation_id == humidity_alert.correlation_id:
            print("✅ Alerts successfully correlated!")
            print(f"Correlation ID: {temp_alert.correlation_id}")
        else:
            print("❌ Alerts not correlated")
    
    print("✅ Correlation testing completed!")

class AlertDashboardData:
    """Helper class to prepare data for dashboard display"""
    
    def __init__(self, alert_system: SmartAlertSystem):
        self.alert_system = alert_system
    
    def get_dashboard_data(self) -> Dict:
        """Get formatted data for dashboard"""
        active_alerts = self.alert_system.get_active_alerts()
        summary = self.alert_system.get_alert_summary()
        
        # Prepare chart data
        severity_chart_data = [
            {'severity': k, 'count': v} 
            for k, v in summary['active_alerts_by_severity'].items()
        ]
        
        # Prepare recent alerts for table
        recent_alerts = []
        for alert in active_alerts[:10]:  # Latest 10 alerts
            recent_alerts.append({
                'id': alert.alert_id,
                'sensor': alert.sensor_id,
                'location': alert.location,
                'type': alert.alert_type.value,
                'severity': alert.severity.value,
                'message': alert.message,
                'timestamp': alert.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'status': alert.status.value,
                'acknowledged': alert.is_acknowledged,
                'escalation_level': alert.escalation_level
            })
        
        # System health metrics
        total_sensors = len(set(alert.sensor_id for alert in self.alert_system.alert_history))
        sensors_with_alerts = len(set(alert.sensor_id for alert in active_alerts))
        system_health = max(0, 100 - (sensors_with_alerts / max(total_sensors, 1) * 100))
        
        return {
            'summary': summary,
            'severity_chart_data': severity_chart_data,
            'recent_alerts': recent_alerts,
            'system_health': round(system_health, 1),
            'maintenance_sensors': list(self.alert_system.maintenance_mode.keys()),
            'escalation_summary': self._get_escalation_summary()
        }
    
    def _get_escalation_summary(self) -> Dict:
        """Get escalation summary"""
        escalated_alerts = [
            alert for alert in self.alert_system.active_alerts.values()
            if alert.escalation_level > 0
        ]
        
        return {
            'total_escalated': len(escalated_alerts),
            'max_escalation_level': max([a.escalation_level for a in escalated_alerts], default=0),
            'critical_escalated': len([a for a in escalated_alerts if a.severity == AlertSeverity.CRITICAL])
        }

# Configuration templates
ALERT_SYSTEM_CONFIG = {
    'escalation_settings': {
        'low_escalation_minutes': 60,
        'medium_escalation_minutes': 30,
        'high_escalation_minutes': 15,
        'critical_escalation_minutes': 5
    },
    'notification_settings': {
        'email_enabled': True,
        'sms_enabled': True,
        'slack_enabled': True,
        'default_recipients': ['facility@company.com']
    },
    'correlation_settings': {
        'correlation_window_minutes': 10,
        'enable_temperature_humidity_correlation': True,
        'enable_co2_occupancy_correlation': True,
        'enable_system_wide_correlation': True
    },
    'maintenance_settings': {
        'default_maintenance_duration_hours': 2,
        'auto_clear_resolved_alerts_hours': 24
    }
}

if __name__ == "__main__":
    # Run tests
    test_alert_system()
    print("\n" + "="*50)
    demo_alert_correlation()
    
    # Demonstrate dashboard data preparation
    print("\n" + "="*50)
    print("📊 Dashboard Data Example:")
    alert_system = SmartAlertSystem()
    
    # Generate some test data
    alert_system.process_sensor_data(
        'hvac_office_a1',
        {'temperature': 30.0, 'humidity': 75.0, 'co2': 1200},
        'Office Floor A1'
    )
    
    dashboard = AlertDashboardData(alert_system)
    dashboard_data = dashboard.get_dashboard_data()
    
    print(f"System Health: {dashboard_data['system_health']}%")
    print(f"Active Alerts: {dashboard_data['summary']['active_alerts_total']}")
    print(f"Recent Alerts: {len(dashboard_data['recent_alerts'])}")
    
    print("\n✅ Smart Alert System Demo Completed!")
    print("\n🏗️ Ready for integration with HVAC monitoring system!")