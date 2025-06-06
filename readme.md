
```
def import_orphans():
    if not Path(ORPHAN_CSV).exists():
        logger.error(f"Orphan CSV not found: {ORPHAN_CSV}")
        return

    inserted = []
    with open(ORPHAN_CSV, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        with conn.cursor() as cursor:
            for row in reader:
                key_field = row.get("orphaned_s3_key") or row.get("expected_key")
                if not key_field:
                    continue

                key = normalize_filename(key_field)
                if key.endswith("/"):
                    logger.info(f"[SKIPPED] Skipping folder marker key: {key}")
                    continue

                filename = key.split("/")[-1]
                new_key = f"{TARGET_PREFIX}orphan/{filename}"

                try:
                    # Check if orphan key already exists
                    try:
                        existing_etag = s3_client.head_object(Bucket=BUCKET_NAME, Key=new_key).get("ETag", "").strip('"')
                        logger.info(f"[SKIPPED COPY] {new_key} already exists in S3")
                    except s3_client.exceptions.ClientError as e:
                        if e.response['ResponseMetadata']['HTTPStatusCode'] == 404:
                            logger.info(f"[COPY] {key} → {new_key}")
                            if not args.dry_run:
                                s3_client.copy_object(
                                    Bucket=BUCKET_NAME,
                                    CopySource={'Bucket': BUCKET_NAME, 'Key': key},
                                    Key=new_key
                                )
                            existing_etag = s3_client.head_object(Bucket=BUCKET_NAME, Key=new_key).get("ETag", "").strip('"')
                        else:
                            logger.error(f"[ERROR] Unexpected error checking/copying {key}: {e}")
                            continue

                    # Delete original object if not dry run
                    if not args.dry_run:
                        s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)

                    # Insert into DB
                    new_url = f"{ENDPOINT_URL.rstrip('/')}/{new_key}"
                    if not args.dry_run:
                        cursor.execute(
                            f"INSERT INTO {TABLE_NAME} (name, url, path, hcp_id) VALUES (%s, %s, %s, %s)",
                            (filename, new_url, new_key, existing_etag)
                        )
                    inserted.append({
                        "name": filename,
                        "path": new_key,
                        "url": new_url,
                        "hcp_id": existing_etag
                    })

                except Exception as e:
                    logger.error(f"[ERROR] Failed to import orphan {key} → {e}")

            if not args.dry_run:
                conn.commit()

    if inserted:
        with open(INSERT_LOG_CSV, "w", newline='', encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=["name", "path", "url", "hcp_id"])
            writer.writeheader()
            for row in inserted:
                writer.writerow(row)
        logger.info(f"Imported {len(inserted)} orphaned files. Log written to {INSERT_LOG_CSV}")
    else:
        logger.info("No orphans were imported.")
















def finalize_bunk_urls():
    logger.info("Finalizing URLs: replacing server.example.com with bunk")

    with conn.cursor() as cursor:
        sql = f"SELECT id, url FROM {TABLE_NAME} WHERE url LIKE %s"
        cursor.execute(sql, ("%server.example.com%",))
        rows = cursor.fetchall()

        updated = 0
        already_bunked = 0
        total_checked = len(rows)

        for row in rows:
            parsed = urllib.parse.urlparse(row["url"])
            if parsed.netloc == "bunk":
                already_bunked += 1
                logger.info(f"[SKIPPED] Already bunked: {row['url']}")
                continue

            new_url = parsed._replace(netloc="bunk").geturl()

            logger.info(f"[BUNK] {row['url']} → {new_url}")
            if not args.dry_run:
                cursor.execute(
                    f"UPDATE {TABLE_NAME} SET url = %s WHERE id = %s",
                    (new_url, row["id"])
                )
                updated += 1

        if not args.dry_run:
            conn.commit()

        logger.info(f"Checked {total_checked} rows total")
        logger.info(f"Skipped {already_bunked} already-bunked URLs")
        logger.info(f"Updated {updated} URLs with new bunk domain")



```




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
from pathlib import Path
from difflib import get_close_matches

# Disable SSL warnings and force UTF-8 output
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# === CONFIGURE THESE ===
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
ORPHAN_CSV = "orphaned_s3_files.csv"
INSERT_LOG_CSV = "imported_orphans_log.csv"
SOURCE_DIR = "/mnt/share/legacy"
TARGET_PREFIX = "legacy/"
BUNK_DOMAIN = "this.was.bunk"

# === CLI ARGS ===
parser = argparse.ArgumentParser(description="HCP S3 Reconciliation Tool")
parser.add_argument('--dry-run', action='store_true', help="Skip DB updates and S3 delete/copy")
parser.add_argument('--log-file', help="Optional log output file")
parser.add_argument('--find-orphans', action='store_true', help="Find S3 files not tracked in DB")
parser.add_argument('--migrate', action='store_true', help="Upload files from local share to S3")
parser.add_argument('--import-orphans', action='store_true', help="Copy and reindex orphaned S3 files")
parser.add_argument('--all', action='store_true', help="Run full pipeline: migrate → reconcile → find-orphans → import-orphans")
args = parser.parse_args()

# === LOGGING ===
handlers = [logging.StreamHandler()]
if args.log_file:
    handlers.append(logging.FileHandler(args.log_file, encoding='utf-8'))
logging.basicConfig(level=logging.INFO, handlers=handlers, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("reconcile")

# === S3 CLIENT ===
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT_URL,
    verify=False
)

# === MYSQL CONNECTION ===
conn = pymysql.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    database=DB_NAME,
    cursorclass=pymysql.cursors.DictCursor
)

# === HELPERS ===
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

def upload_file(file_path, s3_key):
    try:
        s3_client.upload_file(file_path, BUCKET_NAME, s3_key)
        response = s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
        return response.get("ETag", "").strip('"')
    except Exception as e:
        logger.error(f"Failed to upload {file_path} to {s3_key}: {e}")
        return None

# === FIND ORPHANS ===
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
        norm_key = normalize_filename(key.lower())
        if norm_key.endswith("/") or norm_key == PREFIX.lower():
            continue
        if norm_key not in db_paths:
            orphaned.append(key)

    logger.info(f"Found {len(orphaned)} orphaned S3 files.")
    with open(ORPHAN_CSV, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["orphaned_s3_key"])
        for key in orphaned:
            writer.writerow([key])
    logger.info(f"Wrote orphaned keys to {ORPHAN_CSV}")

# === RECONCILE ===
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
            if not url or not url.lower().startswith(("https://", "http://")):
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
                parsed = urllib.parse.urlparse(url)
                bunk_url = parsed._replace(netloc=BUNK_DOMAIN).geturl()
                unmatched.append({"id": row["id"], "url": bunk_url, "expected_key": s3_key})
                logger.warning(f"[MISSING] {file_name} not found → Rewritten URL: {bunk_url}")

    if not args.dry_run:
        conn.commit()

    if unmatched:
        with open(UNMATCHED_OUTPUT, "w", newline='', encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["id", "url", "expected_key"])
            writer.writeheader()
            for row in unmatched:
                writer.writerow(row)
        logger.info(f"Exported {len(unmatched)} unmatched rows to {UNMATCHED_OUTPUT}")

# === MIGRATE ===
def migrate():
    source_path = Path(SOURCE_DIR)
    if not source_path.exists():
        logger.error(f"Source directory does not exist: {SOURCE_DIR}")
        return

    files = [f for f in source_path.iterdir() if f.is_file()]
    logger.info(f"Starting migration of {len(files)} files")

    for file_path in files:
        s3_key = f"{TARGET_PREFIX}{file_path.name}"
        etag = upload_file(str(file_path), s3_key)
        if etag:
            logger.info(f"[UPLOADED] {file_path.name} → {s3_key} [ETag: {etag}]")
        else:
            logger.warning(f"[FAILED] {file_path.name} upload failed.")

    logger.info("Migration completed.")

# === IMPORT ORPHANS ===
def import_orphans():
    if not Path(ORPHAN_CSV).exists():
        logger.error(f"Orphan CSV not found: {ORPHAN_CSV}")
        return

    inserted = []
    with open(ORPHAN_CSV, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        with conn.cursor() as cursor:
            for row in reader:
                key_field = row.get("orphaned_s3_key") or row.get("expected_key")
                if not key_field:
                    continue

                key = normalize_filename(key_field)
                if key.endswith("/"):
                    logger.info(f"[SKIPPED] Skipping folder marker key: {key}")
                    continue

                filename = key.split("/")[-1]
                new_key = f"{TARGET_PREFIX}orphan/{filename}"

                try:
                    logger.info(f"[COPY] {key} → {new_key}")
                    if not args.dry_run:
                        s3_client.copy_object(
                            Bucket=BUCKET_NAME,
                            CopySource={'Bucket': BUCKET_NAME, 'Key': key},
                            Key=new_key
                        )
                        s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)

                        etag = s3_client.head_object(Bucket=BUCKET_NAME, Key=new_key).get("ETag", "").strip('"')
                        new_url = f"{ENDPOINT_URL.rstrip('/')}/{new_key}"

                        cursor.execute(
                            f"INSERT INTO {TABLE_NAME} (name, url, path, hcp_id) VALUES (%s, %s, %s, %s)",
                            (filename, new_url, new_key, etag)
                        )
                        inserted.append({
                            "name": filename,
                            "path": new_key,
                            "url": new_url,
                            "hcp_id": etag
                        })
                except Exception as e:
                    logger.error(f"Failed to import orphan {key} → {e}")

            if not args.dry_run:
                conn.commit()

    if inserted:
        with open(INSERT_LOG_CSV, "w", newline='', encoding="utf-8") as out:
            writer = csv.DictWriter(out, fieldnames=["name", "path", "url", "hcp_id"])
            writer.writeheader()
            for row in inserted:
                writer.writerow(row)
        logger.info(f"Imported {len(inserted)} orphaned files. Log written to {INSERT_LOG_CSV}")
    else:
        logger.info("No orphans were imported.")

# === MAIN ===
if __name__ == "__main__":
    try:
        if args.all:
            migrate()
            reconcile()
            find_orphaned_s3_files()
            import_orphans()
        elif args.import_orphans:
            import_orphans()
        elif args.migrate:
            migrate()
        elif args.find_orphans:
            find_orphaned_s3_files()
        else:
            reconcile()
    finally:
        conn.close()
