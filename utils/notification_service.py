"""
Notification Service for HVAC Monitoring System
שירות התראות למערכת HVAC
"""
import smtplib
import json
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from email.mime.base import MimeBase
from email import encoders
from dataclasses import dataclass, asdict
from enum import Enum
import logging
import threading
import queue

class NotificationStatus(Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"
    RETRY = "RETRY"

class NotificationPriority(Enum):
    LOW = 1
    NORMAL = 2
    HIGH = 3
    URGENT = 4

@dataclass
class NotificationRequest:
    """בקשת התראה"""
    notification_id: str
    channel: str
    recipients: List[str]
    subject: str
    message: str
    priority: NotificationPriority
    created_at: datetime
    metadata: Dict[str, Any]
    retry_count: int = 0
    max_retries: int = 3
    status: NotificationStatus = NotificationStatus.PENDING
    sent_at: Optional[datetime] = None
    error_message: Optional[str] = None

class EmailNotificationChannel:
    """ערוץ התראות אימייל"""
    
    def __init__(self, smtp_server: str, smtp_port: int, username: str, password: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.use_tls = True
        self.logger = logging.getLogger(__name__)
    
    def send_notification(self, request: NotificationRequest) -> bool:
        """שליחת התראת אימייל"""
        try:
            # יצירת הודעה
            msg = MimeMultipart()
            msg['From'] = self.username
            msg['To'] = ', '.join(request.recipients)
            msg['Subject'] = request.subject
            
            # הוספת תוכן
            body = self._format_email_body(request)
            msg.attach(MimeText(body, 'html' if '<html>' in body else 'plain', 'utf-8'))
            
            # חיבור לשרת SMTP
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            
            if self.use_tls:
                server.starttls()
            
            server.login(self.username, self.password)
            
            # שליחת הודעה
            text = msg.as_string()
            server.sendmail(self.username, request.recipients, text)
            server.quit()
            
            self.logger.info(f"📧 Email sent successfully to {request.recipients}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to send email: {e}")
            return False
    
    def _format_email_body(self, request: NotificationRequest) -> str:
        """עיצוב תוכן אימייל"""
        if request.metadata.get('html_format', False):
            return self._format_html_email(request)
        else:
            return self._format_text_email(request)
    
    def _format_text_email(self, request: NotificationRequest) -> str:
        """עיצוב אימייל טקסט"""
        body = f"""
{request.message}

---
HVAC Monitoring System
זמן: {request.created_at.strftime('%Y-%m-%d %H:%M:%S')}
עדיפות: {request.priority.name}
מזהה: {request.notification_id}
"""
        
        # הוספת מטא-דטה
        if 'sensor_id' in request.metadata:
            body += f"חיישן: {request.metadata['sensor_id']}\n"
        
        if 'alert_type' in request.metadata:
            body += f"סוג התראה: {request.metadata['alert_type']}\n"
        
        return body
    
    def _format_html_email(self, request: NotificationRequest) -> str:
        """עיצוב אימייל HTML"""
        priority_color = {
            NotificationPriority.LOW: '#28a745',
            NotificationPriority.NORMAL: '#17a2b8',
            NotificationPriority.HIGH: '#fd7e14',
            NotificationPriority.URGENT: '#dc3545'
        }
        
        color = priority_color.get(request.priority, '#6c757d')
        
        html = f"""
<html>
<body dir="rtl" style="font-family: Arial, sans-serif;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
        <div style="background-color: {color}; color: white; padding: 15px; border-radius: 5px 5px 0 0;">
            <h2 style="margin: 0;">🚨 התראת מערכת HVAC</h2>
        </div>
        
        <div style="border: 1px solid #ddd; padding: 20px; border-radius: 0 0 5px 5px;">
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 15px;">
                <strong>{request.subject}</strong>
            </div>
            
            <p style="font-size: 16px; line-height: 1.6;">
                {request.message.replace('\\n', '<br>')}
            </p>
            
            <table style="width: 100%; margin-top: 20px; border-collapse: collapse;">
                <tr>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd; font-weight: bold;">זמן:</td>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd;">{request.created_at.strftime('%Y-%m-%d %H:%M:%S')}</td>
                </tr>
                <tr>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd; font-weight: bold;">עדיפות:</td>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd;">{request.priority.name}</td>
                </tr>
"""
        
        # הוספת מטא-דטה לטבלה
        if 'sensor_id' in request.metadata:
            html += f"""
                <tr>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd; font-weight: bold;">חיישן:</td>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd;">{request.metadata['sensor_id']}</td>
                </tr>
"""
        
        if 'location' in request.metadata:
            html += f"""
                <tr>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd; font-weight: bold;">מיקום:</td>
                    <td style="padding: 8px; border-bottom: 1px solid #ddd;">{request.metadata['location']}</td>
                </tr>
"""
        
        html += """
            </table>
            
            <div style="margin-top: 20px; padding: 10px; background-color: #e9ecef; border-radius: 5px; font-size: 12px; color: #6c757d;">
                הודעה זו נשלחה אוטומטית ממערכת ניטור HVAC
            </div>
        </div>
    </div>
</body>
</html>
"""
        
        return html

class SlackNotificationChannel:
    """ערוץ התראות Slack"""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.logger = logging.getLogger(__name__)
    
    def send_notification(self, request: NotificationRequest) -> bool:
        """שליחת התראת Slack"""
        try:
            # הכנת payload
            payload = self._prepare_slack_payload(request)
            
            # שליחת בקשה
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                self.logger.info(f"💬 Slack notification sent successfully")
                return True
            else:
                self.logger.error(f"❌ Slack notification failed: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Failed to send Slack notification: {e}")
            return False
    
    def _prepare_slack_payload(self, request: NotificationRequest) -> Dict[str, Any]:
        """הכנת payload ל-Slack"""
        # צבעים לפי עדיפות
        color_map = {
            NotificationPriority.LOW: '#28a745',
            NotificationPriority.NORMAL: '#17a2b8',
            NotificationPriority.HIGH: '#fd7e14',
            NotificationPriority.URGENT: '#dc3545'
        }
        
        # אייקונים לפי עדיפות
        emoji_map = {
            NotificationPriority.LOW: ':information_source:',
            NotificationPriority.NORMAL: ':bell:',
            NotificationPriority.HIGH: ':warning:',
            NotificationPriority.URGENT: ':rotating_light:'
        }
        
        # הכנת שדות
        fields = [
            {
                "title": "זמן",
                "value": request.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                "short": True
            },
            {
                "title": "עדיפות",
                "value": request.priority.name,
                "short": True
            }
        ]
        
        # הוספת מטא-דטה
        if 'sensor_id' in request.metadata:
            fields.append({
                "title": "חיישן",
                "value": request.metadata['sensor_id'],
                "short": True
            })
        
        if 'location' in request.metadata:
            fields.append({
                "title": "מיקום",
                "value": request.metadata['location'],
                "short": True
            })
        
        payload = {
            "text": f"{emoji_map.get(request.priority, ':bell:')} HVAC Alert",
            "attachments": [
                {
                    "color": color_map.get(request.priority, '#6c757d'),
                    "title": request.subject,
                    "text": request.message,
                    "fields": fields,
                    "footer": "HVAC Monitoring System",
                    "ts": int(request.created_at.timestamp())
                }
            ]
        }
        
        return payload

class SMSNotificationChannel:
    """ערוץ התראות SMS (סימולציה)"""
    
    def __init__(self, api_key: str = None, service_url: str = None):
        self.api_key = api_key
        self.service_url = service_url
        self.logger = logging.getLogger(__name__)
    
    def send_notification(self, request: NotificationRequest) -> bool:
        """שליחת התראת SMS (סימולציה)"""
        try:
            # סימולציה של שליחת SMS
            sms_text = self._format_sms_text(request)
            
            # בסימולציה - רק נדפיס ונחזיר הצלחה
            self.logger.info(f"📱 SMS simulation - would send to {request.recipients}")
            self.logger.info(f"📱 SMS content: {sms_text}")
            
            # במימוש אמיתי היה כאן קוד שליחת SMS דרך API
            # response = requests.post(self.service_url, ...)
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to send SMS: {e}")
            return False
    
    def _format_sms_text(self, request: NotificationRequest) -> str:
        """עיצוב טקסט SMS"""
        # SMS מוגבל ל-160 תווים
        text = f"HVAC Alert: {request.subject[:50]}"
        
        if 'sensor_id' in request.metadata:
            text += f" ({request.metadata['sensor_id']})"
        
        text += f" - {request.priority.name}"
        
        # הגבלת אורך
        return text[:160]

class WebhookNotificationChannel:
    """ערוץ התראות Webhook כללי"""
    
    def __init__(self, webhook_url: str, headers: Dict[str, str] = None):
        self.webhook_url = webhook_url
        self.headers = headers or {'Content-Type': 'application/json'}
        self.logger = logging.getLogger(__name__)
    
    def send_notification(self, request: NotificationRequest) -> bool:
        """שליחת התראת Webhook"""
        try:
            payload = {
                'notification_id': request.notification_id,
                'channel': request.channel,
                'recipients': request.recipients,
                'subject': request.subject,
                'message': request.message,
                'priority': request.priority.name,
                'created_at': request.created_at.isoformat(),
                'metadata': request.metadata
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code in [200, 201, 202]:
                self.logger.info(f"🔗 Webhook notification sent successfully")
                return True
            else:
                self.logger.error(f"❌ Webhook notification failed: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Failed to send webhook notification: {e}")
            return False

class NotificationService:
    """שירות התראות ראשי"""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.channels = {}
        self.notification_queue = queue.PriorityQueue()
        self.failed_notifications = queue.Queue()
        
        # Thread management
        self.worker_thread = None
        self.retry_thread = None
        self.is_running = False
        
        # Statistics
        self.stats = {
            'total_sent': 0,
            'total_failed': 0,
            'by_channel': {},
            'by_priority': {}
        }
        
        # Notification history
        self.notification_history = []
        self.max_history = 1000
        
        self.logger = logging.getLogger(__name__)
        
        # Initialize default channels
        self._initialize_channels()
    
    def _initialize_channels(self):
        """אתחול ערוצי התראות"""
        # Email channel
        email_config = self.config.get('email', {})
        if email_config.get('enabled', False):
            self.add_email_channel(
                smtp_server=email_config.get('smtp_server', 'localhost'),
                smtp_port=email_config.get('smtp_port', 587),
                username=email_config.get('username', ''),
                password=email_config.get('password', '')
            )
        
        # Slack channel
        slack_config = self.config.get('slack', {})
        if slack_config.get('enabled', False):
            self.add_slack_channel(
                webhook_url=slack_config.get('webhook_url', '')
            )
        
        # SMS channel
        sms_config = self.config.get('sms', {})
        if sms_config.get('enabled', False):
            self.add_sms_channel(
                api_key=sms_config.get('api_key', ''),
                service_url=sms_config.get('service_url', '')
            )
    
    def add_email_channel(self, smtp_server: str, smtp_port: int, username: str, password: str):
        """הוספת ערוץ אימייל"""
        self.channels['email'] = EmailNotificationChannel(smtp_server, smtp_port, username, password)
        self.logger.info("📧 Email notification channel added")
    
    def add_slack_channel(self, webhook_url: str):
        """הוספת ערוץ Slack"""
        self.channels['slack'] = SlackNotificationChannel(webhook_url)
        self.logger.info("💬 Slack notification channel added")
    
    def add_sms_channel(self, api_key: str, service_url: str):
        """הוספת ערוץ SMS"""
        self.channels['sms'] = SMSNotificationChannel(api_key, service_url)
        self.logger.info("📱 SMS notification channel added")
    
    def add_webhook_channel(self, name: str, webhook_url: str, headers: Dict[str, str] = None):
        """הוספת ערוץ Webhook"""
        self.channels[name] = WebhookNotificationChannel(webhook_url, headers)
        self.logger.info(f"🔗 Webhook notification channel '{name}' added")
    
    def start(self):
        """הפעלת שירות התראות"""
        if self.is_running:
            self.logger.warning("⚠️ Notification service already running")
            return
        
        self.is_running = True
        
        # Worker thread לעיבוד התראות
        self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker_thread.start()
        
        # Retry thread לניסיונות חוזרים
        self.retry_thread = threading.Thread(target=self._retry_loop, daemon=True)
        self.retry_thread.start()
        
        self.logger.info("🚀 Notification service started")
    
    def stop(self):
        """עצירת שירות התראות"""
        self.is_running = False
        
        if self.worker_thread:
            self.worker_thread.join(timeout=5)
        
        if self.retry_thread:
            self.retry_thread.join(timeout=5)
        
        self.logger.info("🛑 Notification service stopped")
    
    def send_notification(self, channel: str, recipients: List[str], subject: str, 
                         message: str, priority: NotificationPriority = NotificationPriority.NORMAL,
                         metadata: Dict[str, Any] = None) -> str:
        """שליחת התראה"""
        # יצירת בקשת התראה
        notification_id = f"notif_{int(time.time())}_{hash(message) % 10000}"
        
        request = NotificationRequest(
            notification_id=notification_id,
            channel=channel,
            recipients=recipients,
            subject=subject,
            message=message,
            priority=priority,
            created_at=datetime.now(),
            metadata=metadata or {}
        )
        
        # הוספה לתור עם עדיפות
        self.notification_queue.put((priority.value, request))
        
        self.logger.info(f"📤 Notification queued: {notification_id}")
        return notification_id
    
    def send_alert_notification(self, alert_data: Dict[str, Any], 
                               channels: List[str] = None) -> List[str]:
        """שליחת התראה על סמך נתוני אלרט"""
        if channels is None:
            channels = list(self.channels.keys())
        
        notification_ids = []
        
        # הכנת נתונים
        subject = f"HVAC Alert: {alert_data.get('alert_type', 'Unknown')}"
        message = alert_data.get('message', 'Alert detected')
        
        # קביעת עדיפות
        severity = alert_data.get('severity', 'MEDIUM')
        priority_map = {
            'LOW': NotificationPriority.LOW,
            'MEDIUM': NotificationPriority.NORMAL,
            'HIGH': NotificationPriority.HIGH,
            'CRITICAL': NotificationPriority.URGENT
        }
        priority = priority_map.get(severity, NotificationPriority.NORMAL)
        
        # קביעת נמענים לפי חומרה
        recipients = self._get_recipients_for_severity(severity)
        
        # מטא-דטה
        metadata = {
            'alert_id': alert_data.get('alert_id'),
            'sensor_id': alert_data.get('sensor_id'),
            'location': alert_data.get('location'),
            'alert_type': alert_data.get('alert_type'),
            'severity': severity,
            'values': alert_data.get('values', {}),
            'html_format': True  # לעיצוב HTML באימייל
        }
        
        # שליחה לכל הערוצים המבוקשים
        for channel in channels:
            if channel in self.channels:
                notification_id = self.send_notification(
                    channel=channel,
                    recipients=recipients,
                    subject=subject,
                    message=message,
                    priority=priority,
                    metadata=metadata
                )
                notification_ids.append(notification_id)
        
        return notification_ids
    
    def _get_recipients_for_severity(self, severity: str) -> List[str]:
        """קביעת נמענים לפי חומרה"""
        recipients_config = self.config.get('recipients', {})
        
        default_recipients = recipients_config.get('default', ['admin@company.com'])
        
        severity_recipients = {
            'LOW': recipients_config.get('low', default_recipients),
            'MEDIUM': recipients_config.get('medium', default_recipients),
            'HIGH': recipients_config.get('high', default_recipients + ['manager@company.com']),
            'CRITICAL': recipients_config.get('critical', default_recipients + ['manager@company.com', 'emergency@company.com'])
        }
        
        return severity_recipients.get(severity, default_recipients)
    
    def _worker_loop(self):
        """לולאת עיבוד התראות"""
        self.logger.info("🔄 Notification worker started")
        
        while self.is_running:
            try:
                # המתנה להתראה (עם timeout)
                try:
                    priority, request = self.notification_queue.get(timeout=1)
                except queue.Empty:
                    continue
                
                # עיבוד ההתראה
                success = self._process_notification(request)
                
                if success:
                    request.status = NotificationStatus.SENT
                    request.sent_at = datetime.now()
                    
                    # עדכון סטטיסטיקות
                    self.stats['total_sent'] += 1
                    self.stats['by_channel'][request.channel] = self.stats['by_channel'].get(request.channel, 0) + 1
                    self.stats['by_priority'][request.priority.name] = self.stats['by_priority'].get(request.priority.name, 0) + 1
                else:
                    request.status = NotificationStatus.FAILED
                    self.stats['total_failed'] += 1
                    
                    # הוספה לתור ניסיונות חוזרים
                    if request.retry_count < request.max_retries:
                        request.retry_count += 1
                        request.status = NotificationStatus.RETRY
                        self.failed_notifications.put(request)
                
                # שמירה בהיסטוריה
                self._add_to_history(request)
                
                self.notification_queue.task_done()
                
            except Exception as e:
                self.logger.error(f"❌ Error in notification worker: {e}")
        
        self.logger.info("⏹️ Notification worker stopped")
    
    def _retry_loop(self):
        """לולאת ניסיונות חוזרים"""
        self.logger.info("🔄 Notification retry worker started")
        
        while self.is_running:
            try:
                # המתנה להתראה כושלת
                try:
                    request = self.failed_notifications.get(timeout=5)
                except queue.Empty:
                    continue
                
                # המתנה לפני ניסיון חוזר (exponential backoff)
                delay = min(2 ** request.retry_count, 60)  # מקסימום דקה
                time.sleep(delay)
                
                # ניסיון חוזר
                success = self._process_notification(request)
                
                if success:
                    request.status = NotificationStatus.SENT
                    request.sent_at = datetime.now()
                    self.stats['total_sent'] += 1
                    self.logger.info(f"✅ Retry successful for {request.notification_id}")
                else:
                    if request.retry_count < request.max_retries:
                        request.retry_count += 1
                        self.failed_notifications.put(request)
                    else:
                        request.status = NotificationStatus.FAILED
                        self.logger.error(f"❌ Max retries reached for {request.notification_id}")
                
                # עדכון בהיסטוריה
                self._update_in_history(request)
                
                self.failed_notifications.task_done()
                
            except Exception as e:
                self.logger.error(f"❌ Error in retry worker: {e}")
        
        self.logger.info("⏹️ Notification retry worker stopped")
    
    def _process_notification(self, request: NotificationRequest) -> bool:
        """עיבוד התראה יחידה"""
        try:
            channel = self.channels.get(request.channel)
            if not channel:
                self.logger.error(f"❌ Unknown notification channel: {request.channel}")
                request.error_message = f"Unknown channel: {request.channel}"
                return False
            
            # שליחת ההתראה
            success = channel.send_notification(request)
            
            if not success:
                request.error_message = "Channel send method returned False"
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Error processing notification {request.notification_id}: {e}")
            request.error_message = str(e)
            return False
    
    def _add_to_history(self, request: NotificationRequest):
        """הוספה להיסטוריית התראות"""
        self.notification_history.append(request)
        
        # הגבלת גודל היסטוריה
        if len(self.notification_history) > self.max_history:
            self.notification_history.pop(0)
    
    def _update_in_history(self, request: NotificationRequest):
        """עדכון רשומה בהיסטוריה"""
        for i, historical_request in enumerate(self.notification_history):
            if historical_request.notification_id == request.notification_id:
                self.notification_history[i] = request
                break
    
    def get_notification_status(self, notification_id: str) -> Optional[NotificationRequest]:
        """קבלת סטטוס התראה"""
        for request in self.notification_history:
            if request.notification_id == notification_id:
                return request
        return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """קבלת סטטיסטיקות שירות"""
        return {
            'total_sent': self.stats['total_sent'],
            'total_failed': self.stats['total_failed'],
            'success_rate': self.stats['total_sent'] / max(self.stats['total_sent'] + self.stats['total_failed'], 1),
            'by_channel': self.stats['by_channel'].copy(),
            'by_priority': self.stats['by_priority'].copy(),
            'queue_size': self.notification_queue.qsize(),
            'retry_queue_size': self.failed_notifications.qsize(),
            'available_channels': list(self.channels.keys()),
            'is_running': self.is_running
        }
    
    def get_recent_notifications(self, limit: int = 50) -> List[Dict[str, Any]]:
        """קבלת התראות אחרונות"""
        recent = self.notification_history[-limit:] if self.notification_history else []
        return [asdict(req) for req in recent]

# פונקציות עזר
def create_notification_service(config: Dict[str, Any] = None) -> NotificationService:
    """יצירת שירות התראות"""
    return NotificationService(config)

def send_test_notification(service: NotificationService, channel: str, recipient: str) -> str:
    """שליחת התראת בדיקה"""
    return service.send_notification(
        channel=channel,
        recipients=[recipient],
        subject="HVAC System Test",
        message="This is a test notification from the HVAC monitoring system.",
        priority=NotificationPriority.LOW,
        metadata={'test': True}
    )

# בדיקה ישירה
if __name__ == "__main__":
    print("🔧 Testing Notification Service...")
    
    # תצורת בדיקה
    config = {
        'email': {
            'enabled': True,
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': 587,
            'username': 'test@example.com',
            'password': 'test_password'
        },
        'slack': {
            'enabled': True,
            'webhook_url': 'https://hooks.slack.com/test'
        },
        'recipients': {
            'default': ['admin@company.com'],
            'high': ['admin@company.com', 'manager@company.com'],
            'critical': ['admin@company.com', 'manager@company.com', 'emergency@company.com']
        }
    }
    
    # יצירת שירות
    service = create_notification_service(config)
    service.start()
    
    # שליחת התראת בדיקה
    test_alert = {
        'alert_id': 'test_001',
        'sensor_id': 'hvac_office_a1',
        'location': 'Office A1',
        'alert_type': 'TEMPERATURE_HIGH',
        'severity': 'HIGH',
        'message': 'Temperature exceeded threshold: 28.5°C',
        'values': {'temperature': 28.5, 'humidity': 65.0}
    }
    
    print("📤 Sending test alert notification...")
    notification_ids = service.send_alert_notification(test_alert, ['email', 'slack'])
    
    print(f"✅ Sent {len(notification_ids)} notifications: {notification_ids}")
    
    # המתנה קצרה
    time.sleep(2)
    
    # בדיקת סטטיסטיקות
    stats = service.get_statistics()
    print(f"\n📊 Notification Statistics:")
    print(f"   Total Sent: {stats['total_sent']}")
    print(f"   Total Failed: {stats['total_failed']}")
    print(f"   Success Rate: {stats['success_rate']:.2%}")
    print(f"   Available Channels: {stats['available_channels']}")
    
    # עצירת השירות
    service.stop()
    
    print("\n✅ Notification service test completed!")