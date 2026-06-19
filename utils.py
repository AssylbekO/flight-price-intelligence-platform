from dotenv import load_dotenv
import os
import boto3
from sqlalchemy import create_engine

load_dotenv()

def get_s3_client():
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_REGION")

    if not aws_access_key or not aws_secret_key or not region:
        raise ValueError("Missing AWS environment variables")

    return boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )

def get_db_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    dbname = os.getenv("POSTGRES_DB")

    if not all([user, password, host, port, dbname]):
        raise ValueError("Missing PostgreSQL environment variables")

    connection_string = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    )

    engine = create_engine(connection_string)
    return engine


if __name__ == "__main__":
    print(os.getenv("POSTGRES_DB"))

    engine = get_db_engine()
    with engine.connect() as conn:
        print("connection successful")

    s3 = get_s3_client()
    print(s3.list_buckets())
