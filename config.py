
import os
from dotenv import load_dotenv

# Load environment variables from .env file
# Look for .env in the same directory (project root)
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(env_path)

# Base directory for the Python source files
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# File paths
INPUT_FILE = os.path.join(BASE_DIR, 'input_usernames.csv')
HISTORY_DIR = os.path.join(BASE_DIR, 'following_history')
OUTPUT_DIR = os.path.join(BASE_DIR, 'new_following')
TWEETS_DIR = os.path.join(BASE_DIR, 'follower_tweets')
INPUT_HISTORY_DIR = os.path.join(BASE_DIR, 'input_history')
PROMPTS_DIR = os.path.join(BASE_DIR, 'prompts')
FOLLOWER_COUNTS_DIR = os.path.join(BASE_DIR, 'follower_counts')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
RAW_RESPONSES_DIR = os.path.join(BASE_DIR, 'raw_api_responses')
AI_TWEETS_DIR = os.path.join(TWEETS_DIR, 'ai_tweets')
DB_DIR = os.path.join(BASE_DIR, 'db')
SCHEMA_SQL = os.path.join(DB_DIR, 'schema.sql')

# Limit how many profiles we process from CSV
MAX_PROFILES = int(os.getenv('MAX_PROFILES', 75))

# RapidAPI configuration
RAPID_API_KEY = os.getenv('RAPID_API_KEY')
RAPID_API_HOST = 'twitter135.p.rapidapi.com'
RAPID_API_REQUESTS_PER_SECOND = int(os.getenv('RAPID_API_REQUESTS_PER_SECOND', 3))
RAPID_API_INTERVAL_MS = 1000 / RAPID_API_REQUESTS_PER_SECOND

# OpenAI configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
OPENAI_MAX_RETRIES = int(os.getenv('OPENAI_MAX_RETRIES', 3))
OPENAI_TIMEOUT_MS = int(os.getenv('OPENAI_TIMEOUT_MS', 120000))
OPENAI_REQUESTS_PER_MINUTE = int(os.getenv('OPENAI_REQUESTS_PER_MINUTE', 60))
OPENAI_THROTTLE_INTERVAL_MS = 60000 / OPENAI_REQUESTS_PER_MINUTE
OPENAI_MODEL = os.getenv('OPENAI_MODEL', 'o3') # Default to 'o3' as per app.js

# Deduplication and filtering
MAX_FOLLOWERS = int(os.getenv('MAX_FOLLOWERS', 1000))
MAX_FOLLOWING = int(os.getenv('MAX_FOLLOWING', 1000))
MAX_ACCOUNT_AGE_DAYS = int(os.getenv('MAX_ACCOUNT_AGE_DAYS', 45))

# Concurrency
MAX_CONCURRENT_REQUESTS = int(os.getenv('MAX_CONCURRENT_REQUESTS', 2)) # For tweet collection
CONCURRENT_PROCESSES = int(os.getenv('CONCURRENT_PROCESSES', 5)) # For AI analysis

# Debug mode
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'

# Notion configuration (add a flag to control uploads)
NOTION_DATABASE_ID = os.getenv('NOTION_DATABASE_ID')
NOTION_API_KEY = os.getenv('NOTION_API_KEY')
NOTION_UPLOAD_ENABLED = os.getenv('NOTION_UPLOAD_ENABLED', 'True').lower() == 'true'

# Email notification
EMAIL_RECIPIENTS_FILE = os.path.join(BASE_DIR, 'email_recipients.txt')
EMAIL_SENDER_EMAIL = os.getenv('EMAIL_SENDER_EMAIL')
EMAIL_SENDER_PASSWORD = os.getenv('EMAIL_SENDER_PASSWORD')
EMAIL_SMTP_SERVER = os.getenv('EMAIL_SMTP_SERVER')
EMAIL_SMTP_PORT = int(os.getenv('EMAIL_SMTP_PORT', 587))

# Recovery file
RECOVERY_FILE = os.path.join(BASE_DIR, 'recovery_state.json')
