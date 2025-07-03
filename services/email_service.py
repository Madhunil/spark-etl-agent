"""
Enhanced email service with production features.
"""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from typing import Union, List, Optional
from loguru import logger
from core.config import get_settings

class EmailService:
    """Production-ready email notification service."""
    
    def __init__(self):
        """Initialize email service."""
        self.settings = get_settings()
        logger.info("📧 Email Service initialized")
    
    def send_email(self, to_email: Union[str, List[str]], subject: str, 
                   body: str, from_email: Optional[str] = None) -> bool:
        """Send email with enhanced error handling."""
        try:
            sender = from_email or self.settings.EMAIL_FROM
            recipients = [to_email] if isinstance(to_email, str) else to_email
            
            logger.info(f"📤 Sending email: '{subject}' to {len(recipients)} recipient(s)")
            
            msg = MIMEMultipart()
            msg['From'] = sender
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP(self.settings.SMTP_SERVER, self.settings.SMTP_PORT) as server:
                if self.settings.SMTP_USE_TLS:
                    server.starttls()
                
                if self.settings.SMTP_USERNAME and self.settings.SMTP_PASSWORD:
                    server.login(self.settings.SMTP_USERNAME, self.settings.SMTP_PASSWORD)
                
                server.sendmail(sender, recipients, msg.as_string())
            
            logger.info("✅ Email sent successfully")
            return True
            
        except Exception as e:
            logger.exception(f"❌ Failed to send email: {str(e)}")
            return False
    
    def send_data_variance_alert(self, variance_percentage: float, job_name: str,
                               previous_count: int, current_count: int) -> bool:
        """Send formatted data variance alert."""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
            
            subject = f"🚨 Data Variance Alert - {job_name}"
            
            body = f"""
🚨 DATA VARIANCE ALERT - IMMEDIATE ATTENTION REQUIRED

Job: {job_name}
Timestamp: {timestamp}

📊 VARIANCE DETAILS:
Previous Count: {previous_count:,} rows
Current Count: {current_count:,} rows
Variance: {variance_percentage:.2f}%
Threshold: {self.settings.DATA_VARIANCE_THRESHOLD}%

⚠️ The data variance exceeds the configured threshold. Please investigate:
• Data source changes
• ETL logic modifications
• Data quality issues
• System performance problems

🔍 RECOMMENDED ACTIONS:
1. Review source data for anomalies
2. Check ETL logs for errors or warnings
3. Validate data transformation logic
4. Compare with historical patterns
5. Contact data engineering team if needed

This is an automated alert from the ETL monitoring system.
Please acknowledge receipt and provide status updates.

Best regards,
DNA JanssenPH Support Team
            """.strip()
            
            return self.send_email(
                to_email=self.settings.EMAIL_TO_DNA_TEAM,
                subject=subject,
                body=body
            )
            
        except Exception as e:
            logger.exception(f"❌ Failed to send variance alert: {str(e)}")
            return False
    
    def send_job_completion_notification(self, job_name: str, status: str,
                                       duration: float, rows_processed: int = 0,
                                       error_message: Optional[str] = None) -> bool:
        """Send job completion notification."""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
            status_emoji = "✅" if status == "Success" else "❌"
            
            subject = f"{status_emoji} {job_name} - {status}"
            
            if status == "Success":
                body = f"""
✅ JOB COMPLETED SUCCESSFULLY

Job: {job_name}
Status: {status}
Completion Time: {timestamp}
Duration: {duration:.2f} seconds
Rows Processed: {rows_processed:,}

The job executed without any issues.

Best regards,
DNA JanssenPH Support Team
                """.strip()
            else:
                body = f"""
❌ JOB EXECUTION FAILED

Job: {job_name}
Status: {status}
Failure Time: {timestamp}
Duration: {duration:.2f} seconds
Error: {error_message or 'Unknown error'}

Please investigate the issue and take corrective action.

Best regards,
DNA JanssenPH Support Team
                """.strip()
            
            return self.send_email(
                to_email=self.settings.EMAIL_TO_DNA_TEAM,
                subject=subject,
                body=body
            )
            
        except Exception as e:
            logger.exception(f"❌ Failed to send job notification: {str(e)}")
            return False