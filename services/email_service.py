import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from utils.logger import logger
from config import (
    EMAIL_RECIPIENTS_FILE,
    EMAIL_SENDER_EMAIL,
    EMAIL_SENDER_PASSWORD,
    EMAIL_SMTP_SERVER,
    EMAIL_SMTP_PORT
)

async def send_completion_email(stats):
    """
    Sends an email notification with the summary of the bot's run.
    """
    if not EMAIL_SENDER_EMAIL or not EMAIL_SENDER_PASSWORD or not EMAIL_SMTP_SERVER:
        logger.warn("Email sending is not fully configured. Skipping email notification.")
        return

    recipients = []
    if os.path.exists(EMAIL_RECIPIENTS_FILE):
        try:
            with open(EMAIL_RECIPIENTS_FILE, 'r') as f:
                recipients = [line.strip() for line in f if line.strip()]
        except Exception as e:
            logger.error(f"Error reading email recipients file: {e}")
            return
    
    if not recipients:
        logger.warn("No email recipients found. Skipping email notification.")
        return

    subject = "Twitter Analysis Bot - Run Completion Summary"
    body = f"""
    Twitter Analysis Bot run completed successfully!

    Summary:
    Total Profiles Processed: {stats.get('totalProcessed', 0)}
    Total Profiles Uploaded to Notion: {stats.get('totalUploaded', 0)}
    Total Profiles Skipped: {stats.get('totalSkipped', 0)}

    For more details, please check the logs.
    """

    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER_EMAIL
    msg['To'] = ", ".join(recipients)
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        logger.log(f"Attempting to send email to {recipients}...")
        with smtplib.SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT) as server:
            server.starttls() # Secure the connection
            server.login(EMAIL_SENDER_EMAIL, EMAIL_SENDER_PASSWORD)
            server.send_message(msg)
        logger.log("Email notification sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send email notification: {e}")

# Expose the function for direct import as in app.js
send_completion_email = send_completion_email