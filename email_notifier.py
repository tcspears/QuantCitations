import smtplib
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from email.mime.text import MIMEText
import settings


def send_notification_email(email_message):
    server = smtplib.SMTP_SSL(host=settings.SENDER_SMTP, port=465)
    server.login(settings.SENDER_ADDRESS, settings.SENDER_PASSWORD)
    msg = MIMEMultipart()
    msg['From'] = settings.SENDER_ADDRESS
    msg['To'] = settings.DESTINATION_ADDRESS
    msg['Subject'] = 'RePEc scraper notification at ' + datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg.attach(MIMEText(email_message, 'plain'))
    server.send_message(msg)
    server.quit()

