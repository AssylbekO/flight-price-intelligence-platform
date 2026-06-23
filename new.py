import os
import io
import zipfile
import logging
import psycopg2
from dotenv import load_dotenv
from utils import get_s3_client

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

S3_BUCKET = os.getenv("S3_BUCKET_NAME")

# Database credentials
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
# Default to localhost for local testing outside Docker, but use 'postgres' inside Airflow
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

def create_raw_table(cur, table_name, headers):
