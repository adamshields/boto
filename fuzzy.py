#!/usr/bin/env python3
import boto3
import pymysql
import urllib.parse
import urllib3
import logging
import argparse
import csv
import unicodedata
import sys
from difflib import get_close_matches

# Disable SSL warnings and fix console encoding
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# CONFIGURE THESE
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

# ARGS
parser = argparse.ArgumentParser(description="Reconcile MySQL entries with HCP S3 bucket + fuzzy fallback")
parser.add_argument('--dry-run', action='store_true', help="Do not write to DB")
parser.add_argument('--log-file', help="Write logs to file")
args = parser.parse_args()

# Logging
handlers = [logging.StreamHandler()]
if args.log_file:
    handlers.append(logging.FileHandler(args.log_file, encoding='utf-8'))
logging.basicConfig(level=logging.INFO, handlers=handlers, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("reconcile")

# S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT_URL,
    verify=False
)

# DB
conn = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME,
    cursorclass=pymysql.cursors.DictCursor
)

# Helpers
def extract_filename_from_url(url):
    try:
        parsed = urllib.parse.urlparse(url)
        filename = parsed.path.split("/")[-1]
        return urllib.parse.unquote(filename)
    except Exception as e:
        logger.warning(f"Bad URL {url}: {e}")
        return None

def normalize_filename(name):
    if not name:
        return None
    return unicodedata.normalize("NFC", name).strip()

def s3_object_exists(key):
    try:
        key_bytes = key.encode("utf-8", errors="replace")
        s3_key_safe = key_bytes.decode("utf-8", errors="replace")
        response = s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key_safe)
        return response.get("ETag", "").strip('"')
    except s3_client.exceptions.ClientError as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            return None
        raise

def fuzzy_lookup(filename, s3_keys, cutoff=0.85):
    matches = get_close_matches(filename, s3_keys, n=1, cutoff=cutoff)
    return matches[0] if matches else None

# Main
def reconcile():
    updated = 0
    unmatched = []

    # Fetch all existing S3 keys (filenames only) under legacy/
    all_s3_keys = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX):
        all_s3_keys.extend([
            obj["Key"].replace(PREFIX, "")
            for obj in page.get("Contents", [])
        ])
    logger.info(f"Cached {len(all_s3_keys)} files from S3 for fuzzy matching")

    with conn.cursor() as cursor:
        cursor.execute(f"SELECT id, url FROM {TABLE_NAME}")
        rows = cursor.fetchall()

        for row in rows:
            url = row["url"]
            if not url or not url.startswith("https://server/Artifacts/"):
                continue

            raw_filename = extract_filename_from_url(url)
            file_name = normalize_filename(raw_filename)
            if not file_name:
                continue

            s3_key = normalize_filename(f"{PREFIX}{file_name}")
            etag = s3_object_exists(s3_key)

            if not etag:
                # Fuzzy fallback
                fuzzy_match = fuzzy_lookup(file_name, all_s3_keys)
                if fuzzy_match:
                    fuzzy_key = normalize_filename(f"{PREFIX}{fuzzy_match}")
                    logger.warning(f"[FUZZY] {file_name} ≈ {fuzzy_match}")
                    etag = s3_object_exists(fuzzy_key)
                    if etag:
                        s3_key = fuzzy_key  # Use fuzzy match instead

            if etag:
                logger.info(f"[MATCHED] {file_name} → {s3_key}")
                if not args.dry_run:
                    cursor.execute(
                        f"UPDATE {TABLE_NAME} SET hcp_id = %s, path = %s WHERE id = %s",
                        (etag, s3_key, row["id"])
                    )
                    updated += 1
            else:
                unmatched.append({"id": row["id"], "url": url, "expected_key": s3_key})
                logger.warning(f"[MISSING] {file_name} not found → Raw: {repr(file_name)}")

        if not args.dry_run:
            conn.commit()

    if unmatched:
        with open(UNMATCHED_OUTPUT, "w", newline='', encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["id", "url", "expected_key"])
            writer.writeheader()
            for row in unmatched:
                row = {k: str(v).encode("utf-8", errors="replace").decode("utf-8") for k, v in row.items()}
                writer.writerow(row)
        logger.info(f"Exported {len(unmatched)} unmatched rows to {UNMATCHED_OUTPUT}")

    logger.info(f"Updated {updated} rows in DB (dry run: {args.dry_run})")

if __name__ == "__main__":
    try:
        reconcile()
    finally:
        conn.close()
