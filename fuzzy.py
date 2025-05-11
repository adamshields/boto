#!/usr/bin/env python3
import os
import sys
import csv
import time
import boto3
import pymysql
import logging
import urllib3
import argparse
import unicodedata
from pathlib import Path
from difflib import get_close_matches

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# === CONFIGURATION ===
DB_HOST = "your-mysql-host"
DB_USER = "your-user"
DB_PASS = "your-password"
DB_NAME = "your-db"
TABLE_NAME = "your-table"

BUCKET_NAME = "adam"
ENDPOINT_URL = "https://your-hcp-endpoint.com"
ACCESS_KEY = "your-s3-access-key"
SECRET_KEY = "your-s3-secret-key"

SOURCE_DIR = "Y:/path/to/share"
TARGET_PREFIX = "legacy/"
ORPHAN_CSV = "orphaned_s3_files.csv"
INSERT_LOG_CSV = "imported_orphan_files.csv"
UNMATCHED_OUTPUT = "unmatched_files.csv"

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("s3tool.log"), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# === S3 + DB ===
s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT_URL,
    verify=False
)

db = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME,
    cursorclass=pymysql.cursors.DictCursor
)

# === HELPERS ===
def normalize_filename(name):
    if not name:
        return None
    return unicodedata.normalize("NFC", name).strip()

def s3_object_exists(key):
    try:
        response = s3.head_object(Bucket=BUCKET_NAME, Key=key)
        return response.get("ETag", "").strip('"')
    except s3.exceptions.ClientError as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            return None
        raise

def upload_file(local_path, s3_key):
    try:
        with open(local_path, 'rb') as data:
            response = s3.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=data
            )
        return response.get('ETag', '').strip('"')
    except Exception as e:
        logger.error(f"Error uploading {local_path}: {e}")
        return None

# === MIGRATE FILES ===
def migrate(args):
    source_path = Path(SOURCE_DIR)
    files = [f for f in source_path.iterdir() if f.is_file()]
    logger.info(f"Starting migration of {len(files)} files")
    for file_path in files:
        s3_key = f"{TARGET_PREFIX}{file_path.name}"
        etag = upload_file(str(file_path), s3_key)
        if etag:
            logger.info(f"Uploaded {file_path.name} to {s3_key} [ETag: {etag}]")
    logger.info("Migration completed.")

# === RECONCILE DB ===
def reconcile(args):
    updated = 0
    unmatched = []
    all_s3_keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=TARGET_PREFIX):
        all_s3_keys.extend([
            obj["Key"].replace(TARGET_PREFIX, "")
            for obj in page.get("Contents", [])
        ])

    with db.cursor() as cursor:
        cursor.execute(f"SELECT id, url FROM {TABLE_NAME}")
        rows = cursor.fetchall()
        for row in rows:
            url = row["url"]
            if not url or not url.lower().startswith(("http://server/artifacts/", "https://server/artifacts/")):
                continue
            filename = normalize_filename(url.split("/")[-1])
            s3_key = f"{TARGET_PREFIX}{filename}"
            etag = s3_object_exists(s3_key)
            if not etag:
                match = get_close_matches(filename, all_s3_keys, n=1, cutoff=0.85)
                if match:
                    s3_key = f"{TARGET_PREFIX}{match[0]}"
                    etag = s3_object_exists(s3_key)
            if etag:
                new_url = f"{ENDPOINT_URL.rstrip('/')}/{s3_key}"
                cursor.execute(
                    f"UPDATE {TABLE_NAME} SET hcp_id = %s, path = %s, url = %s WHERE id = %s",
                    (etag, s3_key, new_url, row["id"])
                )
                updated += 1
            else:
                unmatched.append({"id": row["id"], "url": url, "expected_key": s3_key})
        db.commit()

    with open(UNMATCHED_OUTPUT, "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["id", "url", "expected_key"])
        writer.writeheader()
        for row in unmatched:
            writer.writerow(row)

    logger.info(f"Reconciled {updated} rows. Unmatched written to {UNMATCHED_OUTPUT}")

# === FIND ORPHANS ===
def find_orphans(args):
    with db.cursor() as cursor:
        cursor.execute(f"SELECT path FROM {TABLE_NAME} WHERE path IS NOT NULL")
        db_paths = set(normalize_filename(row['path'].lower()) for row in cursor.fetchall())

    s3_keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=TARGET_PREFIX):
        s3_keys.extend(obj["Key"] for obj in page.get("Contents", []))

    orphaned = [k for k in s3_keys if normalize_filename(k.lower()) not in db_paths]

    with open(ORPHAN_CSV, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["orphaned_s3_key"])
        for key in orphaned:
            writer.writerow([key])

    logger.info(f"Found {len(orphaned)} orphaned S3 files. Wrote to {ORPHAN_CSV}")

# === IMPORT ORPHANS ===
def import_orphans(args):
    with open(ORPHAN_CSV, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        inserted = []
        with db.cursor() as cursor:
            for row in reader:
                key = normalize_filename(row["orphaned_s3_key"])
                filename = key.split("/")[-1]
                new_key = f"{TARGET_PREFIX}orphan/{filename}"
                try:
                    s3.copy_object(
                        Bucket=BUCKET_NAME,
                        CopySource={'Bucket': BUCKET_NAME, 'Key': key},
                        Key=new_key
                    )
                    s3.delete_object(Bucket=BUCKET_NAME, Key=key)

                    new_url = f"{ENDPOINT_URL.rstrip('/')}/{new_key}"
                    etag = s3.head_object(Bucket=BUCKET_NAME, Key=new_key).get("ETag", "").strip('"')

                    cursor.execute(
                        f"INSERT INTO {TABLE_NAME} (name, url, path, hcp_id) VALUES (%s, %s, %s, %s)",
                        (filename, new_url, new_key, etag)
                    )
                    inserted.append({"name": filename, "path": new_key, "url": new_url, "hcp_id": etag})
                except Exception as e:
                    logger.error(f"Failed to import orphan: {key} → {e}")
            db.commit()

        with open(INSERT_LOG_CSV, "w", newline='', encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=["name", "path", "url", "hcp_id"])
            writer.writeheader()
            for row in inserted:
                writer.writerow(row)

        logger.info(f"Imported {len(inserted)} orphaned files. Log written to {INSERT_LOG_CSV}")

# === MAIN ===
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified S3 + DB Migration & Reconciliation Tool")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("migrate", help="Upload all files from source directory to S3")
    sub.add_parser("reconcile-db", help="Update DB with hcp_id/path from existing S3 files")
    sub.add_parser("find-orphans", help="Find S3 files not tracked in DB")
    sub.add_parser("import-orphans", help="Move orphan files and insert into DB with hcp_id")

    args = parser.parse_args()

    try:
        if args.command == "migrate":
            migrate(args)
        elif args.command == "reconcile-db":
            reconcile(args)
        elif args.command == "find-orphans":
            find_orphans(args)
        elif args.command == "import-orphans":
            import_orphans(args)
        else:
            parser.print_help()
    finally:
        db.close()














































#!/usr/bin/env python3
import boto3
import pymysql
import csv
import logging
import urllib3
import unicodedata

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# CONFIGURATION
DB_HOST = "your-mysql-host"
DB_USER = "your-user"
DB_PASS = "your-password"
DB_NAME = "your-db"
TABLE_NAME = "your-table"
BUCKET_NAME = "adam"
ENDPOINT_URL = "https://your-hcp-endpoint.com"
ORPHAN_CSV = "orphaned_s3_files.csv"
INSERT_LOG_CSV = "imported_orphan_files.csv"

# LOGGING
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("import_orphans")

# CONNECT TO DB
conn = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME,
    cursorclass=pymysql.cursors.DictCursor
)

# S3 CLIENT
s3 = boto3.client(
    's3',
    aws_access_key_id="your-s3-access-key",
    aws_secret_access_key="your-s3-secret-key",
    endpoint_url=ENDPOINT_URL,
    verify=False
)

def normalize_filename(name):
    if not name:
        return None
    return unicodedata.normalize("NFC", name).strip()

def move_and_import():
    with open(ORPHAN_CSV, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        inserted = []

        with conn.cursor() as cursor:
            for row in reader:
                key = normalize_filename(row["orphaned_s3_key"])
                filename = key.split("/")[-1]
                new_key = f"legacy/orphan/{filename}"

                try:
                    # Copy and delete original
                    s3.copy_object(
                        Bucket=BUCKET_NAME,
                        CopySource={'Bucket': BUCKET_NAME, 'Key': key},
                        Key=new_key
                    )
                    s3.delete_object(Bucket=BUCKET_NAME, Key=key)

                    # Insert into DB
                    hcp_url = f"{ENDPOINT_URL.rstrip('/')}/{new_key}"
                    cursor.execute(
                        f"INSERT INTO {TABLE_NAME} (name, url, path) VALUES (%s, %s, %s)",
                        (filename, hcp_url, new_key)
                    )
                    inserted.append({"name": filename, "path": new_key, "url": hcp_url})
                    logger.info(f"Imported {filename} → {new_key}")

                except Exception as e:
                    logger.error(f"Failed to move or insert {key}: {e}")

            conn.commit()

        with open(INSERT_LOG_CSV, "w", newline='', encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=["name", "path", "url"])
            writer.writeheader()
            for row in inserted:
                writer.writerow(row)

        logger.info(f"Imported {len(inserted)} orphaned files into DB and moved to /legacy/orphan/")
        logger.info(f"Wrote import log to {INSERT_LOG_CSV}")

if __name__ == "__main__":
    try:
        move_and_import()
    finally:
        conn.close()

























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

# Disable SSL warnings and force UTF-8 output
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

# CLI ARGS
parser = argparse.ArgumentParser(description="HCP S3 Reconciliation Tool")
parser.add_argument('--dry-run', action='store_true', help="Skip DB updates")
parser.add_argument('--log-file', help="Optional log output file")
parser.add_argument('--find-orphans', action='store_true', help="Find S3 files not tracked in DB")
args = parser.parse_args()

# LOGGING
handlers = [logging.StreamHandler()]
if args.log_file:
    handlers.append(logging.FileHandler(args.log_file, encoding='utf-8'))
logging.basicConfig(level=logging.INFO, handlers=handlers, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("reconcile")

# S3 CLIENT
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT_URL,
    verify=False
)

# MYSQL CONNECTION
conn = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME,
    cursorclass=pymysql.cursors.DictCursor
)

# HELPERS
def extract_filename_from_url(url):
    try:
        filename = url.split("/")[-1]
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
        response = s3_client.head_object(Bucket=BUCKET_NAME, Key=key)
        return response.get("ETag", "").strip('"')
    except s3_client.exceptions.ClientError as e:
        if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
            return None
        raise

def fuzzy_lookup(filename, s3_keys, cutoff=0.85):
    matches = get_close_matches(filename, s3_keys, n=1, cutoff=cutoff)
    return matches[0] if matches else None

def find_orphaned_s3_files():
    logger.info("Checking for orphaned files in S3...")

    with conn.cursor() as cursor:
        cursor.execute(f"SELECT path FROM {TABLE_NAME} WHERE path IS NOT NULL")
        db_paths = set(normalize_filename(row['path'].lower()) for row in cursor.fetchall())

    s3_keys = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX):
        s3_keys.extend(obj["Key"] for obj in page.get("Contents", []))

    orphaned = []
    for key in s3_keys:
        if normalize_filename(key.lower()) not in db_paths:
            orphaned.append(key)

    logger.info(f"Found {len(orphaned)} orphaned S3 files.")
    with open("orphaned_s3_files.csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["orphaned_s3_key"])
        for key in orphaned:
            writer.writerow([key])
    logger.info("Wrote orphaned keys to orphaned_s3_files.csv")

def reconcile():
    updated = 0
    unmatched = []

    all_s3_keys = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX):
        all_s3_keys.extend([
            obj["Key"].replace(PREFIX, "")
            for obj in page.get("Contents", [])
        ])
    logger.info(f"Cached {len(all_s3_keys)} S3 keys")

    with conn.cursor() as cursor:
        cursor.execute(f"SELECT id, url FROM {TABLE_NAME}")
        rows = cursor.fetchall()

        for row in rows:
            url = row["url"]
            if not url or not url.lower().startswith(("https://server/artifacts/", "http://server/artifacts/")):
                continue

            raw_filename = extract_filename_from_url(url)
            file_name = normalize_filename(raw_filename)
            if not file_name:
                continue

            s3_key = normalize_filename(f"{PREFIX}{file_name}")
            etag = s3_object_exists(s3_key)

            if not etag:
                fuzzy_match = fuzzy_lookup(file_name, all_s3_keys)
                if fuzzy_match:
                    fuzzy_key = normalize_filename(f"{PREFIX}{fuzzy_match}")
                    logger.warning(f"[FUZZY] {file_name} ≈ {fuzzy_match}")
                    etag = s3_object_exists(fuzzy_key)
                    if etag:
                        s3_key = fuzzy_key

            if etag:
                logger.info(f"[MATCHED] {file_name} → {s3_key}")
                if not args.dry_run:
                    new_url = f"{ENDPOINT_URL.rstrip('/')}/{s3_key}"
                    cursor.execute(
                        f"UPDATE {TABLE_NAME} SET hcp_id = %s, path = %s, url = %s WHERE id = %s",
                        (etag, s3_key, new_url, row["id"])
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

# MAIN
if __name__ == "__main__":
    try:
        if args.find_orphans:
            find_orphaned_s3_files()
        else:
            reconcile()
    finally:
        conn.close()
