#!/usr/bin/env python3
import boto3
import pymysql
import urllib.parse
import logging
import argparse
import csv

# --- CONFIGURE THESE ---
DB_HOST = "your-mysql-host"
DB_USER = "your-user"
DB_PASS = "your-password"
DB_NAME = "your-db"
TABLE_NAME = "your-table"

BUCKET_NAME = "adam"
PREFIX = "legacy/"
ACCESS_KEY = "your-s3-access-key"
SECRET_KEY = "your-s3-secret-key"
ENDPOINT_URL = "https://your-hcp-endpoint.com"

UNMATCHED_OUTPUT = "unmatched_files.csv"

# --- SETUP ARGPARSE ---
parser = argparse.ArgumentParser(description="Reconcile MySQL table entries with HCP S3 bucket")
parser.add_argument('--dry-run', action='store_true', help="Perform a dry run without modifying the database")
parser.add_argument('--log-file', help="Optional path to log file")
args = parser.parse_args()

# --- SETUP LOGGING ---
log_handlers = [logging.StreamHandler()]
if args.log_file:
    log_handlers.append(logging.FileHandler(args.log_file))

logging.basicConfig(level=logging.INFO, handlers=log_handlers,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("reconcile")

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT_URL,
    verify=False
)

# Connect to MySQL
conn = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME,
    cursorclass=pymysql.cursors.DictCursor
)

def extract_filename_from_url(url):
    try:
        parsed = urllib.parse.urlparse(url)
        return parsed.path.split("/")[-1]
    except Exception as e:
        logger.warning(f"Could not extract filename from {url}: {e}")
        return None

def s3_object_exists(key):
    try:
        response = s3_client.head_object(Bucket=BUCKET_NAME, Key=key)
        return response.get("ETag", "").strip('"')
    except s3_client.exceptions.ClientError as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            return None
        raise

def reconcile():
    updated = 0
    unmatched = []

    with conn.cursor() as cursor:
        cursor.execute(f"SELECT id, url FROM {TABLE_NAME}")
        rows = cursor.fetchall()

        for row in rows:
            url = row["url"]
            if not url.startswith("https://server/Artifacts/"):
                continue

            file_name = extract_filename_from_url(url)
            if not file_name:
                continue

            s3_key = f"{PREFIX}{file_name}"
            etag = s3_object_exists(s3_key)

            if etag:
                logger.info(f"Matched: {file_name} → {s3_key} [etag: {etag}]")
                if not args.dry_run:
                    cursor.execute(
                        f"UPDATE {TABLE_NAME} SET hcp_id = %s, path = %s WHERE id = %s",
                        (etag, s3_key, row["id"])
                    )
                    updated += 1
            else:
                unmatched.append({"id": row["id"], "url": url, "expected_key": s3_key})
                logger.warning(f"No match in S3 for {file_name}")

        if not args.dry_run:
            conn.commit()

    if unmatched:
        with open(UNMATCHED_OUTPUT, "w", newline='', encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["id", "url", "expected_key"])
            writer.writeheader()
            writer.writerows(unmatched)
        logger.info(f"Exported {len(unmatched)} unmatched entries to {UNMATCHED_OUTPUT}")

    logger.info(f"Reconciled {updated} files and updated DB. (Dry run: {args.dry_run})")

if __name__ == "__main__":
    try:
        reconcile()
    finally:
        conn.close()
