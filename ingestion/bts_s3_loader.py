import os
import csv
import io
import zipfile
import logging
import psycopg2
from dotenv import load_dotenv
from utils import get_s3_client

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

S3_BUCKET = os.getenv("S3_BUCKET_NAME")
DB_USER   = os.getenv("POSTGRES_USER")
DB_PASS   = os.getenv("POSTGRES_PASSWORD")
DB_NAME   = os.getenv("POSTGRES_DB")
DB_HOST   = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT   = os.getenv("POSTGRES_PORT", "5432")


# ---------------------------------------------------------------------------
# 1. Database connection
# ---------------------------------------------------------------------------

def get_db_connection():
    """
    Returns a raw psycopg2 connection to PostgreSQL.
    We use psycopg2 directly (not SQLAlchemy) because copy_expert()
    is a psycopg2-specific method that SQLAlchemy doesn't expose.
    """
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )


# ---------------------------------------------------------------------------
# 2. DDL — schema and table setup
# ---------------------------------------------------------------------------

def create_raw_table(cur, table_name: str, headers: list):
    """
    Creates the raw schema and the target table.

    All columns are TEXT — we store exactly what BTS gives us.
    Type casting (INT, NUMERIC, DATE) happens later in dbt staging models.

    We DROP before CREATE to make this function idempotent — safe to re-run
    if a previous load failed halfway through.
    """
    # Create schema if it doesn't exist
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    # Replace empty column names with a placeholder
    # BTS CSVs have a trailing comma that produces an unnamed last column
    clean_headers = [col.strip() if col.strip() else "_empty" for col in headers]

    columns_def = ", ".join([f'"{col}" TEXT' for col in clean_headers])
    cur.execute(f"CREATE TABLE {table_name} ({columns_def});")
    logging.info(f"Table {table_name} created with {len(clean_headers)} columns.")


# ---------------------------------------------------------------------------
# 3. ZIP extraction
# ---------------------------------------------------------------------------

def extract_csv_from_zip(zip_bytes: bytes) -> io.TextIOWrapper:
    """
    Opens a ZIP from raw bytes and returns a text stream pointing at the CSV.

    Returns a TextIOWrapper — caller is responsible for reading lines.
    We do NOT read any lines here so the caller controls where the stream starts.
    """
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))

    csv_files = [name for name in z.namelist() if name.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError("No CSV found inside ZIP from S3.")

    csv_filename = csv_files[0]
    logging.info(f"Found CSV inside ZIP: {csv_filename}")

    # TextIOWrapper converts the byte stream to a text stream psycopg2 can read
    return io.TextIOWrapper(z.open(csv_filename), encoding='utf-8')


# ---------------------------------------------------------------------------
# 4. Main load function
# ---------------------------------------------------------------------------

def load_s3_zip_to_postgres(year: int, quarter: int, s3_client):
    """
    Full pipeline for one quarter:
        S3 (ZIP) → extract CSV in memory → create raw table → COPY into PostgreSQL

    Idempotent: safe to re-run. The table is dropped and recreated each time.
    If anything fails, the transaction is rolled back — no partial data left behind.
    """
    s3_key     = f"raw/bts_db1b/year={year}/quarter={quarter}/db1b_{year}_{quarter}.zip"
    table_name = "raw.bts_db1b"

    # --- Step 1: fetch ZIP from S3 ---
    logging.info(f"Fetching s3://{S3_BUCKET}/{s3_key} ...")
    response    = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
    zip_bytes   = response['Body'].read()
    logging.info(f"ZIP fetched ({len(zip_bytes) / 1_000_000:.1f} MB)")

    # --- Step 2: extract CSV stream from ZIP ---
    # --- Step 3: read header line and build table ---
    # We read line 1 manually to get column names.
    # This advances the stream past line 1, so COPY will start from line 2 (first data row).
    # That means we do NOT use "WITH CSV HEADER" in the COPY command —
    # the header is already consumed and the table already knows its columns.
    text_stream = extract_csv_from_zip(zip_bytes)  # for header reading
    text_stream2 = extract_csv_from_zip(zip_bytes)  # for COPY

    # --- Step 3: read headers ---
    reader = csv.reader(text_stream)
    headers = next(reader)
    logging.info(f"CSV has {len(headers)} columns. First few: {headers[:5]}")

    # --- Step 4: load into PostgreSQL with full transaction safety ---
    conn = None
    try:
        conn = get_db_connection()
        cur  = conn.cursor()

        # Create schema + drop old table + create new table
        create_raw_table(cur, table_name, headers)

        # COPY streams the full CSV including header line.
        # PostgreSQL skips the header automatically via WITH CSV HEADER.
        logging.info(f"Bulk loading into {table_name} via COPY ...")
        copy_sql = f'COPY {table_name} FROM STDIN WITH CSV HEADER'
        cur.copy_expert(sql=copy_sql, file=text_stream2)

        # Commit only after COPY completes successfully — all or nothing
        conn.commit()
        logging.info(f"Successfully loaded {year} Q{quarter} into {table_name}.")

    except Exception as e:
        # If anything fails, roll back the entire transaction.
        # The table will not exist in a broken partial state.
        if conn:
            conn.rollback()
            logging.error("Transaction rolled back due to error.")
        logging.error(f"Load failed: {e}")
        raise

    finally:
        # Always close the connection — even if an exception was raised
        if conn:
            conn.close()


# ---------------------------------------------------------------------------
# 5. Entry point for local testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    s3 = get_s3_client()
    load_s3_zip_to_postgres(year=2023, quarter=1, s3_client=s3)