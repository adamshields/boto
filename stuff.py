#!/usr/bin/env python3
import os
import sys
import boto3
import logging
import urllib3
import argparse
from pathlib import Path
from botocore.exceptions import ClientError

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global configuration
ACCESS_KEY = "adam"
SECRET_KEY = "secretkeypass"
ENDPOINT_URL = "https://example.com"
BUCKET_NAME = "adam"
SOURCE_DIR = "Y:/path/to/share"  # Your network share path
TARGET_PREFIX = "new-adam"  # Folder name in HCP bucket

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("migration.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT_URL,
    verify=False  # Disable SSL verification
)


def create_folder(folder_name):
    """Create a new folder in the bucket"""
    # Ensure the folder name ends with a slash
    if not folder_name.endswith('/'):
        folder_name += '/'
        
    try:
        response = s3_client.put_object(Bucket=BUCKET_NAME, Key=folder_name)
        hcp_id = response.get('ETag', '').strip('"')
        logger.info(f"Created folder {BUCKET_NAME}/{folder_name} with HCP ID: {hcp_id}")
        return True
    except Exception as e:
        logger.error(f"Error creating folder {folder_name}: {e}")
        return False


def list_objects(prefix="", max_items=None):
    """List objects in a folder with optional limit"""
    logger.info(f"Listing objects in {BUCKET_NAME}/{prefix}")
    
    kwargs = {
        'Bucket': BUCKET_NAME,
        'Prefix': prefix
    }
    
    if max_items:
        kwargs['MaxKeys'] = max_items

    try:
        response = s3_client.list_objects_v2(**kwargs)
        if 'Contents' in response:
            for obj in response['Contents']:
                logger.info(f"{obj['LastModified']} {obj['Size']:10d} {obj['Key']}")
            return len(response['Contents'])
        else:
            logger.info("No objects found.")
            return 0
    except Exception as e:
        logger.error(f"Error listing objects: {e}")
        return 0


def delete_file(key):
    """Delete a specific file"""
    try:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)
        logger.info(f"Deleted {BUCKET_NAME}/{key}")
        return True
    except Exception as e:
        logger.error(f"Error deleting {key}: {e}")
        return False


def delete_folder(prefix):
    """Delete a folder and all its contents"""
    if not prefix.endswith('/'):
        prefix += '/'
        
    logger.info(f"Deleting folder {BUCKET_NAME}/{prefix} and all its contents")
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)
        
        deleted_count = 0
        for page in page_iterator:
            if 'Contents' in page:
                objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                if objects_to_delete:
                    s3_client.delete_objects(
                        Bucket=BUCKET_NAME,
                        Delete={'Objects': objects_to_delete}
                    )
                    deleted_count += len(objects_to_delete)
                    logger.info(f"Deleted {len(objects_to_delete)} objects in this batch")
        
        logger.info(f"Total objects deleted: {deleted_count}")
        return deleted_count > 0
    except Exception as e:
        logger.error(f"Error deleting folder {prefix}: {e}")
        return False


def upload_file(local_path, s3_key):
    """Upload a single file and log its HCP ID"""
    try:
        with open(local_path, 'rb') as data:
            response = s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=data
            )
        
        hcp_id = response.get('ETag', '').strip('"')
        file_size = os.path.getsize(local_path) / (1024 * 1024)  # Size in MB
        
        logger.info(f"Uploaded {local_path} ({file_size:.2f} MB) to {BUCKET_NAME}/{s3_key} with HCP ID: {hcp_id}")
        return True
    except Exception as e:
        logger.error(f"Error uploading {local_path}: {e}")
        return False


def get_folder_stats(prefix=""):
    """Get total count and size of files in a folder"""
    if not prefix.endswith('/'):
        prefix += '/'
        
    logger.info(f"Getting stats for {BUCKET_NAME}/{prefix}")
    
    try:
        total_size = 0
        total_count = 0
        
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)
        
        for page in page_iterator:
            if 'Contents' in page:
                total_count += len(page['Contents'])
                for obj in page['Contents']:
                    total_size += obj['Size']
        
        total_size_gb = total_size / (1024 ** 3)
        
        logger.info(f"Total Files: {total_count}")
        logger.info(f"Total Size: {total_size_gb:.2f} GB")
        
        return total_count, total_size_gb
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return 0, 0


def copy_files(source_dir, target_prefix, limit=None):
    """Copy files from source to HCP bucket with optional limit"""
    source_path = Path(source_dir)
    
    if not source_path.exists():
        logger.error(f"Source directory doesn't exist: {source_dir}")
        return 0
    
    # Ensure target prefix ends with slash
    if not target_prefix.endswith('/'):
        target_prefix += '/'
    
    # Create the target folder
    create_folder(target_prefix)
    
    try:
        # Get all files in the directory (non-recursive)
        files = [f for f in source_path.iterdir() if f.is_file()]
        
        # Apply limit if specified
        if limit:
            files = files[:limit]
            logger.info(f"Will copy {len(files)} files (limited to {limit})")
        else:
            logger.info(f"Will copy all {len(files)} files")
        
        # Track progress
        success_count = 0
        total_size_mb = 0
        
        # Copy each file
        for file_path in files:
            s3_key = f"{target_prefix}{file_path.name}"
            if upload_file(str(file_path), s3_key):
                success_count += 1
                total_size_mb += file_path.stat().st_size / (1024 * 1024)
        
        logger.info(f"Successfully copied {success_count}/{len(files)} files ({total_size_mb:.2f} MB)")
        return success_count
    except Exception as e:
        logger.error(f"Error copying files: {e}")
        return 0


def test_migration():
    """Test migration with small batch of files"""
    logger.info("=== STARTING TEST MIGRATION ===")
    logger.info(f"Source: {SOURCE_DIR}")
    logger.info(f"Destination: {BUCKET_NAME}/{TARGET_PREFIX}")
    
    # Create the target folder if it doesn't exist
    create_folder(TARGET_PREFIX)
    
    # Copy a limited number of files (test batch)
    copied = copy_files(SOURCE_DIR, TARGET_PREFIX, limit=20)
    
    # Get stats of the test migration
    count, size_gb = get_folder_stats(TARGET_PREFIX)
    
    logger.info(f"Test migration complete: {copied} files copied, total size {size_gb:.2f} GB")
    logger.info("=== TEST MIGRATION FINISHED ===")
    
    return copied > 0


def full_migration():
    """Run full migration of all files"""
    logger.info("=== STARTING FULL MIGRATION ===")
    logger.info(f"Source: {SOURCE_DIR}")
    logger.info(f"Destination: {BUCKET_NAME}/{TARGET_PREFIX}")
    
    # Create the target folder if it doesn't exist
    create_folder(TARGET_PREFIX)
    
    # Copy all files (no limit)
    copied = copy_files(SOURCE_DIR, TARGET_PREFIX)
    
    # Get stats of the full migration
    count, size_gb = get_folder_stats(TARGET_PREFIX)
    
    logger.info(f"Full migration complete: {copied} files copied, total size {size_gb:.2f} GB")
    logger.info("=== FULL MIGRATION FINISHED ===")
    
    return copied > 0


def clean_up():
    """Delete the target folder and its contents"""
    logger.info(f"Cleaning up by deleting {TARGET_PREFIX} folder and all its contents")
    result = delete_folder(TARGET_PREFIX)
    if result:
        logger.info("Cleanup successful")
    else:
        logger.error("Cleanup failed")
    return result


if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='HCP S3 Migration Tool')
    
    # Create mutually exclusive group for primary actions
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument('--test', action='store_true', help='Test migration with 20 files')
    action_group.add_argument('--copy', action='store_true', help='Copy all files (full migration)')
    action_group.add_argument('--list', action='store_true', help='List files in destination')
    action_group.add_argument('--stats', action='store_true', help='Show statistics for destination')
    action_group.add_argument('--clean', action='store_true', help='Delete destination folder and all contents')
    action_group.add_argument('--delete-file', metavar='KEY', help='Delete a specific file by key')
    
    # Additional options
    parser.add_argument('--limit', type=int, help='Limit number of files (for --list or custom --test)')
    parser.add_argument('--source', help='Override source directory')
    parser.add_argument('--target', help='Override target folder')
    
    args = parser.parse_args()
    
    # Override globals if specified
    if args.source:
        SOURCE_DIR = args.source
        logger.info(f"Source directory override: {SOURCE_DIR}")
    
    if args.target:
        TARGET_PREFIX = args.target
        logger.info(f"Target folder override: {TARGET_PREFIX}")
    
    # Execute the requested action
    if args.test:
        if args.limit:
            logger.info(f"Custom test with {args.limit} files")
            copy_files(SOURCE_DIR, TARGET_PREFIX, limit=args.limit)
        else:
            test_migration()
    
    elif args.copy:
        full_migration()
    
    elif args.list:
        list_objects(TARGET_PREFIX, args.limit)
    
    elif args.stats:
        get_folder_stats(TARGET_PREFIX)
    
    elif args.clean:
        clean_up()
    
    elif args.delete_file:
        delete_file(args.delete_file)
