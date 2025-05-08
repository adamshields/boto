def generate_presigned_url(key, expires_in=3600):
    """Generate a presigned URL for a file in HCP S3"""
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': BUCKET_NAME, 'Key': key},
            ExpiresIn=expires_in
        )
        logger.info(f"Generated presigned URL for {key}")
        print(f"Presigned URL (expires in {expires_in}s):\n{url}")
        return url
    except ClientError as e:
        logger.error(f"Error generating presigned URL for {key}: {e}")
        return None

def generate_presigned_urls_for_folder(prefix=None, expires_in=3600):
    """Generate presigned URLs for all files in a folder"""
    if prefix is None:
        prefix = TARGET_PREFIX

    if not prefix.endswith('/'):
        prefix += '/'

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

        count = 0
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    generate_presigned_url(key, expires_in)
                    count += 1
        logger.info(f"Generated {count} presigned URLs from folder {prefix}")
        return count
    except Exception as e:
        logger.error(f"Error generating presigned URLs for folder {prefix}: {e}")
        return 0


# Add these to your argument parser block:

    action_group.add_argument('--presign', metavar='KEY', help='Generate a presigned URL for a specific file')
    action_group.add_argument('--presign-folder', metavar='PREFIX', help='Generate presigned URLs for all files in a folder')


# Add these to your command execution block:

    elif args.presign:
        generate_presigned_url(args.presign)

    elif args.presign_folder:
        generate_presigned_urls_for_folder(args.presign_folder)





# # Create a folder
# python myscript.py --create-folder

# # Create a custom folder
# python myscript.py --create-folder --target "custom-folder"

# # List all folders in the bucket
# python myscript.py --list-folders

# # Test migration with 20 files
# python myscript.py --test

# # Run full migration
# python myscript.py --copy

# # List contents of the target folder 
# python myscript.py --list

# # Get statistics
# python myscript.py --stats

# # Clean up (delete folder and all contents, with confirmation)
# python myscript.py --clean

# # Clean up with auto-confirmation (for scripts)
# python myscript.py --clean -y

#!/usr/bin/env python3
import os
import sys
import boto3
import logging
import urllib3
import argparse
import time
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


def create_folder(folder_name=None):
    """Create a new folder in the bucket"""
    # Use TARGET_PREFIX if no folder name provided
    if folder_name is None:
        folder_name = TARGET_PREFIX
        
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


def list_objects(prefix=None, max_items=None):
    """List objects in a folder with optional limit"""
    # Use TARGET_PREFIX if no prefix provided
    if prefix is None:
        prefix = TARGET_PREFIX
        
    # Ensure prefix ends with slash if it's not empty
    if prefix and not prefix.endswith('/'):
        prefix += '/'
        
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
            print(f"{'Last Modified':<25} {'Size (MB)':<12} {'Key'}")
            print("-" * 80)
            
            for obj in response['Contents']:
                size_mb = obj['Size'] / (1024 * 1024)
                timestamp = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                print(f"{timestamp:<25} {size_mb:<12.2f} {obj['Key']}")
                
                # Also log to file
                logger.info(f"{timestamp} {obj['Size']:10d} {obj['Key']}")
                
            return len(response['Contents'])
        else:
            print("No objects found.")
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
        print(f"Deleted {BUCKET_NAME}/{key}")
        return True
    except Exception as e:
        logger.error(f"Error deleting {key}: {e}")
        print(f"Error deleting {key}: {e}")
        return False


def delete_folder(prefix=None):
    """Delete a folder and all its contents"""
    # Use TARGET_PREFIX if no prefix provided
    if prefix is None:
        prefix = TARGET_PREFIX
        
    # Ensure prefix ends with slash
    if not prefix.endswith('/'):
        prefix += '/'
        
    logger.info(f"Deleting folder {BUCKET_NAME}/{prefix} and all its contents")
    print(f"Deleting folder {BUCKET_NAME}/{prefix} and all its contents...")
    
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
                    
                    # Show progress for large folders
                    if deleted_count % 100 == 0:
                        print(f"Deleted {deleted_count} objects so far...")
        
        # Delete the folder itself (the trailing slash object)
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=prefix)
        
        logger.info(f"Total objects deleted: {deleted_count}")
        print(f"Successfully deleted {deleted_count} objects plus the folder itself.")
        return deleted_count > 0
    except Exception as e:
        logger.error(f"Error deleting folder {prefix}: {e}")
        print(f"Error deleting folder: {e}")
        return False


def upload_file(local_path, s3_key):
    """Upload a single file and log its HCP ID"""
    try:
        file_size = os.path.getsize(local_path) / (1024 * 1024)  # Size in MB
        
        with open(local_path, 'rb') as data:
            response = s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=data
            )
        
        hcp_id = response.get('ETag', '').strip('"')
        
        logger.info(f"Uploaded {local_path} ({file_size:.2f} MB) to {BUCKET_NAME}/{s3_key} with HCP ID: {hcp_id}")
        return True
    except Exception as e:
        logger.error(f"Error uploading {local_path}: {e}")
        return False


def get_folder_stats(prefix=None):
    """Get total count and size of files in a folder"""
    # Use TARGET_PREFIX if no prefix provided
    if prefix is None:
        prefix = TARGET_PREFIX
        
    # Ensure prefix ends with slash if it's not empty
    if prefix and not prefix.endswith('/'):
        prefix += '/'
        
    logger.info(f"Getting stats for {BUCKET_NAME}/{prefix}")
    print(f"Calculating statistics for {BUCKET_NAME}/{prefix}...")
    
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
        
        total_size_mb = total_size / (1024 * 1024)
        total_size_gb = total_size / (1024 ** 3)
        
        logger.info(f"Total Files: {total_count}")
        logger.info(f"Total Size: {total_size_gb:.2f} GB ({total_size_mb:.2f} MB)")
        
        print(f"Total Files: {total_count}")
        print(f"Total Size: {total_size_gb:.2f} GB ({total_size_mb:.2f} MB)")
        
        return total_count, total_size_gb
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        print(f"Error getting statistics: {e}")
        return 0, 0


def copy_files(source_dir=None, target_prefix=None, limit=None):
    """Copy files from source to HCP bucket with optional limit"""
    # Use globals if not provided
    if source_dir is None:
        source_dir = SOURCE_DIR
    
    if target_prefix is None:
        target_prefix = TARGET_PREFIX
    
    source_path = Path(source_dir)
    
    if not source_path.exists():
        logger.error(f"Source directory doesn't exist: {source_dir}")
        print(f"Error: Source directory doesn't exist: {source_dir}")
        return 0
    
    # Ensure target prefix ends with slash
    if not target_prefix.endswith('/'):
        target_prefix += '/'
    
    # Create the target folder if it doesn't exist
    create_folder(target_prefix)
    
    try:
        # Get all files in the directory (non-recursive)
        files = [f for f in source_path.iterdir() if f.is_file()]
        
        # Apply limit if specified
        if limit:
            files = files[:limit]
            logger.info(f"Will copy {len(files)} files (limited to {limit})")
            print(f"Will copy {len(files)} files (limited to {limit})")
        else:
            logger.info(f"Will copy all {len(files)} files")
            print(f"Will copy all {len(files)} files")
        
        # Track progress
        success_count = 0
        total_size_mb = 0
        start_time = time.time()
        
        # Copy each file
        for i, file_path in enumerate(files):
            s3_key = f"{target_prefix}{file_path.name}"
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            
            # Show progress for larger batches
            if len(files) > 10 and i % 5 == 0:
                elapsed = time.time() - start_time
                if elapsed > 0 and i > 0:
                    files_per_sec = i / elapsed
                    eta_seconds = (len(files) - i) / files_per_sec if files_per_sec > 0 else 0
                    eta_min = eta_seconds / 60
                    
                    print(f"Progress: {i}/{len(files)} files ({(i/len(files)*100):.1f}%) - ETA: {eta_min:.1f} minutes")
            
            if upload_file(str(file_path), s3_key):
                success_count += 1
                total_size_mb += file_size_mb
        
        elapsed = time.time() - start_time
        speed_mbps = total_size_mb / elapsed if elapsed > 0 else 0
        
        logger.info(f"Successfully copied {success_count}/{len(files)} files ({total_size_mb:.2f} MB)")
        logger.info(f"Transfer speed: {speed_mbps:.2f} MB/s")
        
        print(f"Successfully copied {success_count}/{len(files)} files ({total_size_mb:.2f} MB)")
        print(f"Transfer speed: {speed_mbps:.2f} MB/s")
        
        return success_count
    except Exception as e:
        logger.error(f"Error copying files: {e}")
        print(f"Error copying files: {e}")
        return 0


def test_migration():
    """Test migration with small batch of files"""
    logger.info("=== STARTING TEST MIGRATION ===")
    print("\n=== STARTING TEST MIGRATION ===")
    print(f"Source: {SOURCE_DIR}")
    print(f"Destination: {BUCKET_NAME}/{TARGET_PREFIX}")
    
    # Create the target folder if it doesn't exist
    create_folder()
    
    # Copy a limited number of files (test batch)
    copied = copy_files(limit=20)
    
    # Get stats of the test migration
    count, size_gb = get_folder_stats()
    
    logger.info(f"Test migration complete: {copied} files copied, total size {size_gb:.2f} GB")
    print(f"\nTest migration complete: {copied} files copied, total size {size_gb:.2f} GB")
    print("=== TEST MIGRATION FINISHED ===")
    
    return copied > 0


def full_migration():
    """Run full migration of all files"""
    logger.info("=== STARTING FULL MIGRATION ===")
    print("\n=== STARTING FULL MIGRATION ===")
    print(f"Source: {SOURCE_DIR}")
    print(f"Destination: {BUCKET_NAME}/{TARGET_PREFIX}")
    
    # Create the target folder if it doesn't exist
    create_folder()
    
    # Copy all files (no limit)
    copied = copy_files()
    
    # Get stats of the full migration
    count, size_gb = get_folder_stats()
    
    logger.info(f"Full migration complete: {copied} files copied, total size {size_gb:.2f} GB")
    print(f"\nFull migration complete: {copied} files copied, total size {size_gb:.2f} GB")
    print("=== FULL MIGRATION FINISHED ===")
    
    return copied > 0


def clean_up():
    """Delete the target folder and its contents"""
    logger.info(f"Cleaning up by deleting {TARGET_PREFIX} folder and all its contents")
    print(f"Cleaning up by deleting {TARGET_PREFIX} folder and all its contents")
    
    result = delete_folder()
    
    if result:
        logger.info("Cleanup successful")
        print("Cleanup successful")
    else:
        logger.error("Cleanup failed")
        print("Cleanup failed")
    
    return result


def list_folders():
    """List all top-level folders in the bucket"""
    logger.info(f"Listing folders in {BUCKET_NAME}")
    print(f"Listing folders in {BUCKET_NAME}:")
    
    try:
        # List all objects
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Delimiter='/'  # Use delimiter to get folders
        )
        
        # Check for folders (common prefixes)
        if 'CommonPrefixes' in response:
            print("\nFolders:")
            print("-" * 40)
            
            for prefix in response['CommonPrefixes']:
                folder = prefix.get('Prefix', '')
                print(f"- {folder}")
                logger.info(f"Found folder: {folder}")
            
            return len(response['CommonPrefixes'])
        else:
            print("No folders found.")
            logger.info("No folders found.")
            return 0
    except Exception as e:
        logger.error(f"Error listing folders: {e}")
        print(f"Error listing folders: {e}")
        return 0


if __name__ == "__main__":
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='HCP S3 Migration Tool')
    
    # Create mutually exclusive group for primary actions
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument('--test', action='store_true', help='Test migration with 20 files')
    action_group.add_argument('--copy', action='store_true', help='Copy all files (full migration)')
    action_group.add_argument('--list', action='store_true', help='List files in destination')
    action_group.add_argument('--list-folders', action='store_true', help='List all folders in the bucket')
    action_group.add_argument('--stats', action='store_true', help='Show statistics for destination')
    action_group.add_argument('--clean', action='store_true', help='Delete destination folder and all contents')
    action_group.add_argument('--create-folder', action='store_true', help='Create a folder')
    action_group.add_argument('--delete-file', metavar='KEY', help='Delete a specific file by key')
    
    # Additional options
    parser.add_argument('--limit', type=int, help='Limit number of files (for --list or custom --test)')
    parser.add_argument('--source', help='Override source directory')
    parser.add_argument('--target', help='Override target folder')
    parser.add_argument('--yes', '-y', action='store_true', help='Auto-confirm dangerous operations like clean or delete')
    
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
            copy_files(limit=args.limit)
        else:
            test_migration()
    
    elif args.copy:
        full_migration()
    
    elif args.list:
        list_objects(max_items=args.limit)
    
    elif args.list_folders:
        list_folders()
    
    elif args.stats:
        get_folder_stats()
    
    elif args.clean:
        if args.yes or input(f"Are you sure you want to DELETE ALL contents in {TARGET_PREFIX}? (y/n): ").lower() == 'y':
            clean_up()
        else:
            print("Cleanup cancelled.")
    
    elif args.create_folder:
        folder_name = args.target if args.target else TARGET_PREFIX
        create_folder(folder_name)
        print(f"Folder {BUCKET_NAME}/{folder_name} created.")
    
    elif args.delete_file:
        if args.yes or input(f"Are you sure you want to delete {args.delete_file}? (y/n): ").lower() == 'y':
            delete_file(args.delete_file)
        else:
            print("File deletion cancelled.")
    
    elif args.delete_file:
        delete_file(args.delete_file)
