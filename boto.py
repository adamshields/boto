#!/bin/bash
# Don't set -e yet so we can continue even with errors
set +e

echo "[INFO] Starting git metadata script"
echo "[DEBUG] Current directory: $(pwd)"

# Check if WORKSPACE is defined and try both with and without it
echo "[DEBUG] WORKSPACE variable is: ${WORKSPACE:-'not set'}"

# Try to find ALL git executables on the system with timeout
echo "[DEBUG] Searching for all git executables (60-second timeout)..."
which git
timeout 60 find / -name git -type f -executable 2>/dev/null | grep -v "/\.git/" || echo "[DEBUG] Search timed out or no results found"

# Check specifically for the EFS git you mentioned
if [ -x "/efs/path/to/git" ]; then
  echo "[DEBUG] Found EFS git at /efs/path/to/git"
else
  echo "[DEBUG] EFS git not found at /efs/path/to/git - adjust path as needed"
fi

# First try without changing directory
echo "[DEBUG] Testing git commands in current directory"
git rev-parse --is-inside-work-tree || echo "[DEBUG] Not in a git repository currently"

# Then try using workspace
if [ -n "$WORKSPACE" ]; then
  echo "[DEBUG] Changing to WORKSPACE: $WORKSPACE"
  cd "$WORKSPACE"
  echo "[DEBUG] Now in: $(pwd)"
  echo "[DEBUG] Testing git in WORKSPACE"
  git rev-parse --is-inside-work-tree || echo "[DEBUG] WORKSPACE is not a git repository"
fi

# Try testing both git executables
SYSTEM_GIT=$(which git 2>/dev/null || echo "/usr/bin/git")
EFS_GIT="/efs/path/to/git"  # Adjust this path to the one you see

echo "[DEBUG] Testing with system git: $SYSTEM_GIT"
if [ -x "$SYSTEM_GIT" ]; then
  $SYSTEM_GIT rev-parse --is-inside-work-tree || echo "[DEBUG] Not a git repo with system git"
else
  echo "[DEBUG] System git not found or not executable"
fi

echo "[DEBUG] Testing with EFS git: $EFS_GIT"
if [ -x "$EFS_GIT" ]; then
  $EFS_GIT rev-parse --is-inside-work-tree || echo "[DEBUG] Not a git repo with EFS git"
else
  echo "[DEBUG] EFS git not found or not executable"
fi

# See what git config we have
echo "[DEBUG] Git config:"
git config --list || echo "[DEBUG] Could not get git config"

# Look for .git directory with timeout
echo "[DEBUG] Looking for .git directory (30-second timeout):"
timeout 30 find . -name .git -type d -maxdepth 3 || echo "[DEBUG] Search timed out or no .git directory found within 3 levels"

# Now proceed with the actual script, but with more diagnostics
echo "[INFO] Selecting git executable..."
if [ -x "$EFS_GIT" ]; then
  GIT_CMD="$EFS_GIT"
elif [ -x "$SYSTEM_GIT" ]; then
  GIT_CMD="$SYSTEM_GIT"
else
  echo "[ERROR] No git executable found"
  exit 1
fi
echo "[INFO] Using git executable: $GIT_CMD"

# Create output directory and JSON stub even if git fails
mkdir -p resources
OUTPUT="resources/git-metadata.json"

# Try to get git data but with fallbacks
echo "[INFO] Attempting to retrieve git metadata..."
COMMIT_HASH=$($GIT_CMD rev-parse HEAD 2>&1) || COMMIT_HASH="unknown-$(date +%s)"
BRANCH_NAME=$($GIT_CMD rev-parse --abbrev-ref HEAD 2>&1) || BRANCH_NAME="unknown"
COMMIT_MSG=$($GIT_CMD log -1 --pretty=format:%B 2>&1) || COMMIT_MSG="unknown"
COMMITTER_NAME=$($GIT_CMD log -1 --pretty=format:%an 2>&1) || COMMITTER_NAME="unknown"
COMMITTER_EMAIL=$($GIT_CMD log -1 --pretty=format:%ae 2>&1) || COMMITTER_EMAIL="unknown"
COMMIT_DATE=$($GIT_CMD log -1 --pretty=format:%cd 2>&1) || COMMIT_DATE="unknown"
LATEST_TAG=$($GIT_CMD describe --tags --abbrev=0 2>/dev/null) || LATEST_TAG="none"

# Build timestamp
BUILD_TIMESTAMP=$(date +"%Y%m%d%H%M%S")

# Escape quotes in text fields
COMMIT_MSG=$(echo "$COMMIT_MSG" | sed 's/"/\\"/g')
COMMITTER_NAME=$(echo "$COMMITTER_NAME" | sed 's/"/\\"/g')

# Write JSON
echo "[INFO] Writing JSON data to $OUTPUT"
cat > "$OUTPUT" << EOF
{
  "ci_version": "${BUILD_VERSION:-unknown}",
  "ci_buildNumber": "${BUILD_NUMBER:-unknown}",
  "ci_buildTimestamp": "$BUILD_TIMESTAMP",
  "branchName": "$BRANCH_NAME",
  "commitHash": "$COMMIT_HASH",
  "commitMessage": "$COMMIT_MSG",
  "committerName": "$COMMITTER_NAME",
  "committerEmail": "$COMMITTER_EMAIL",
  "commitDate": "$COMMIT_DATE",
  "latestTag": "$LATEST_TAG"
}
EOF

echo "[INFO] Script completed. Check debug output above for clues."

























































































#!/bin/bash
set -e

echo "[INFO] Starting git metadata script"

# Make sure we're in the workspace
if [ -n "$WORKSPACE" ]; then
  cd "$WORKSPACE"
  echo "[INFO] Changed to workspace: $WORKSPACE"
fi

# Find git executable with fallbacks
echo "[INFO] Locating git executable..."
GIT_CMD=$(which git 2>/dev/null || echo "/efs/path/to/git" || echo "/usr/bin/git")
if [ ! -x "$GIT_CMD" ]; then
  echo "[ERROR] Git command not found. Please check git installation."
  exit 1
fi
echo "[INFO] Using git executable: $GIT_CMD"

# Get Git data with error handling
echo "[INFO] Retrieving git metadata..."
COMMIT_HASH=$($GIT_CMD rev-parse HEAD 2>&1) || { 
  echo "[WARNING] Failed to get commit hash"; 
  COMMIT_HASH="unknown"; 
}

BRANCH_NAME=$($GIT_CMD rev-parse --abbrev-ref HEAD 2>&1) || { 
  echo "[WARNING] Failed to get branch name"; 
  BRANCH_NAME="unknown"; 
}

COMMIT_MSG=$($GIT_CMD log -1 --pretty=format:%B 2>&1) || { 
  echo "[WARNING] Failed to get commit message"; 
  COMMIT_MSG="unknown"; 
}

COMMITTER_NAME=$($GIT_CMD log -1 --pretty=format:%an 2>&1) || { 
  echo "[WARNING] Failed to get committer name"; 
  COMMITTER_NAME="unknown"; 
}

COMMITTER_EMAIL=$($GIT_CMD log -1 --pretty=format:%ae 2>&1) || { 
  echo "[WARNING] Failed to get committer email"; 
  COMMITTER_EMAIL="unknown"; 
}

COMMIT_DATE=$($GIT_CMD log -1 --pretty=format:%cd 2>&1) || { 
  echo "[WARNING] Failed to get commit date"; 
  COMMIT_DATE="unknown"; 
}

LATEST_TAG=$($GIT_CMD describe --tags --abbrev=0 2>/dev/null) || { 
  echo "[INFO] No git tags found"; 
  LATEST_TAG="none"; 
}

# Build timestamp
BUILD_TIMESTAMP=$(date +"%Y%m%d%H%M%S")

# Ensure output directory exists
mkdir -p resources
OUTPUT="resources/git-metadata.json"
echo "[INFO] Output file will be: $OUTPUT"

# Escape quotes in text fields
COMMIT_MSG=$(echo "$COMMIT_MSG" | sed 's/"/\\"/g')
COMMITTER_NAME=$(echo "$COMMITTER_NAME" | sed 's/"/\\"/g')

# Write JSON
echo "[INFO] Writing JSON data to output file"
cat > "$OUTPUT" << EOF
{
  "ci_version": "${BUILD_VERSION:-unknown}",
  "ci_buildNumber": "${BUILD_NUMBER:-unknown}",
  "ci_buildTimestamp": "$BUILD_TIMESTAMP",
  "branchName": "$BRANCH_NAME",
  "commitHash": "$COMMIT_HASH",
  "commitMessage": "$COMMIT_MSG",
  "committerName": "$COMMITTER_NAME",
  "committerEmail": "$COMMITTER_EMAIL",
  "commitDate": "$COMMIT_DATE",
  "latestTag": "$LATEST_TAG"
}
EOF

echo "[INFO] Metadata generation completed successfully"



































































# Create a folder with access credentials
aws s3api put-object --bucket your-bucket-name --key adam/ --content-length 0 --endpoint-url https://your-hcp-endpoint --no-verify-ssl --aws-access-key-id YOUR_ACCESS_KEY --aws-secret-access-key YOUR_SECRET_KEY

# List bucket contents with access credentials
aws s3 ls s3://your-bucket-name/ --endpoint-url https://your-hcp-endpoint --no-verify-ssl --aws-access-key-id YOUR_ACCESS_KEY --aws-secret-access-key YOUR_SECRET_KEY

# Delete the empty folder with access credentials
aws s3api delete-object --bucket your-bucket-name --key adam/ --endpoint-url https://your-hcp-endpoint --no-verify-ssl --aws-access-key-id YOUR_ACCESS_KEY --aws-secret-access-key YOUR_SECRET_KEY

# Delete all contents of a folder recursively with access credentials
aws s3 rm s3://your-bucket-name/adam/ --recursive --endpoint-url https://your-hcp-endpoint --no-verify-ssl --aws-access-key-id YOUR_ACCESS_KEY --aws-secret-access-key YOUR_SECRET_KEY

# Create a folder
aws s3api put-object --bucket your-bucket-name --key adam/ --content-length 0 --endpoint-url https://your-hcp-endpoint --no-verify-ssl

# List bucket contents
aws s3 ls s3://your-bucket-name/ --endpoint-url https://your-hcp-endpoint --no-verify-ssl

# Create a test file in the folder
aws s3api put-object --bucket your-bucket-name --key adam/test.txt --body test.txt --endpoint-url https://your-hcp-endpoint --no-verify-ssl

# List folder contents
aws s3 ls s3://your-bucket-name/adam/ --endpoint-url https://your-hcp-endpoint --no-verify-ssl

# Delete a single file in the folder
aws s3api delete-object --bucket your-bucket-name --key adam/test.txt --endpoint-url https://your-hcp-endpoint --no-verify-ssl

# Delete all contents of a folder recursively
aws s3 rm s3://your-bucket-name/adam/ --recursive --endpoint-url https://your-hcp-endpoint --no-verify-ssl

# Delete the empty folder
aws s3api delete-object --bucket your-bucket-name --key adam/ --endpoint-url https://your-hcp-endpoint --no-verify-ssl
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import boto3
from botocore.config import Config
import urllib3
import random
import string
import os
import io
import re

# Suppress urllib3 warnings for self-signed/internal certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# AWS S3/HCP credentials and endpoint
aws_access_key = 'your-access-key'
aws_secret_access_key = 'your-secret-key'
endpoint_url = 'https://your-hcp-endpoint'  # Update this
bucket_name = 'your-bucket-name'

hcp_config = Config(
    signature_version='s3v4',
    s3={'addressing_style': 'path'}
)

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_access_key,
    endpoint_url=endpoint_url,
    config=hcp_config,
    verify=False  # Set to internal CA path if needed
)

app = Flask(__name__)
app.secret_key = 'supersecretkey'

def list_files_in_folder(prefix):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        folders = [cp['Prefix'] for cp in response.get('CommonPrefixes', [])]
        files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'] != prefix]
        return folders, files
    except Exception as e:
        flash(f'Error listing files: {e}', 'danger')
        return [], []

def generate_random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_jibberish_content():
    return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + ' ', k=100))

def get_breadcrumbs(prefix):
    if not prefix:
        return []
    parts = prefix.strip('/').split('/')
    breadcrumbs = [{'name': 'Home', 'prefix': ''}]
    for i, part in enumerate(parts):
        breadcrumbs.append({'name': part, 'prefix': '/'.join(parts[:i + 1]) + '/'})
    return breadcrumbs

@app.route('/')
@app.route('/<path:prefix>')
def index(prefix=''):
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    folders, files = list_files_in_folder(prefix)
    breadcrumbs = get_breadcrumbs(prefix)
    return render_template('index.html', folders=folders, files=files, prefix=prefix, breadcrumbs=breadcrumbs, bucket_name=bucket_name)

@app.route('/create_folder', methods=['POST'])
def create_folder():
    folder_name = request.form.get('folder_name', '').strip()
    prefix = request.form.get('prefix', '').strip()

    if not folder_name:
        flash('Folder name cannot be empty.', 'danger')
        return redirect(url_for('index', prefix=prefix))

    if not re.match(r'^[\w\- ]+$', folder_name):
        flash('Folder name contains invalid characters.', 'danger')
        return redirect(url_for('index', prefix=prefix))

    if prefix and not prefix.endswith('/'):
        prefix += '/'

    new_folder_key = f'{prefix}{folder_name}/'

    try:
        # Create an empty folder marker object using a prepared request
        empty_body = b''
        s3.put_object(Bucket=bucket_name, Key=new_folder_key, Body=empty_body)
        flash('Folder created successfully.', 'success')
    except Exception as e:
        flash(f'Error creating folder: {e}', 'danger')

    return redirect(url_for('index', prefix=prefix))

@app.route('/upload_file', methods=['POST'])
def upload_file():
    file = request.files['file']
    prefix = request.form['prefix']
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    file_key = f'{prefix}{file.filename}'
    
    try:
        # Read file data
        file_data = file.read()
        
        # Create a temporary file to get accurate size
        with open('temp_file_upload', 'wb') as f:
            f.write(file_data)
        
        # Upload using the file path instead of fileobj
        s3.upload_file('temp_file_upload', bucket_name, file_key)
        
        # Clean up temp file
        os.remove('temp_file_upload')
        
        flash('File uploaded successfully.', 'success')
    except Exception as e:
        flash(f'Error uploading file: {e}', 'danger')
        # Clean up temp file in case of error
        if os.path.exists('temp_file_upload'):
            os.remove('temp_file_upload')
    
    return redirect(url_for('index', prefix=prefix))

@app.route('/create_file', methods=['POST'])
def create_file():
    file_name = request.form['file_name']
    file_content = request.form['file_content']
    prefix = request.form['prefix']
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    file_key = f'{prefix}{file_name}'
    
    try:
        # Write content to a temporary file
        with open('temp_file_create', 'w', encoding='utf-8') as f:
            f.write(file_content)
        
        # Upload using the file path
        s3.upload_file('temp_file_create', bucket_name, file_key)
        
        # Clean up temp file
        os.remove('temp_file_create')
        
        flash('File created successfully.', 'success')
    except Exception as e:
        flash(f'Error creating file: {e}', 'danger')
        # Clean up temp file in case of error
        if os.path.exists('temp_file_create'):
            os.remove('temp_file_create')
    
    return redirect(url_for('index', prefix=prefix))

@app.route('/edit_file/<path:key>', methods=['GET', 'POST'])
def edit_file(key):
    if request.method == 'POST':
        new_content = request.form['file_content']
        
        try:
            # Write content to a temporary file
            with open('temp_file_edit', 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            # Upload using the file path
            s3.upload_file('temp_file_edit', bucket_name, key)
            
            # Clean up temp file
            os.remove('temp_file_edit')
            
            flash('File updated successfully.', 'success')
        except Exception as e:
            flash(f'Error updating file: {e}', 'danger')
            # Clean up temp file in case of error
            if os.path.exists('temp_file_edit'):
                os.remove('temp_file_edit')
        
        return redirect(url_for('index', prefix='/'.join(key.split('/')[:-1])))
    else:
        try:
            response = s3.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
        except Exception as e:
            flash(f'Error reading file: {e}', 'danger')
            content = ''
        return render_template('edit.html', key=key, content=content)

@app.route('/delete/<path:key>')
def delete_file_or_folder(key):
    try:
        if key.endswith('/'):
            # Handle folder deletion with pagination and batch delete
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=key)
            
            total_deleted = 0
            for page in pages:
                if 'Contents' in page and page['Contents']:
                    objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                    if objects_to_delete:
                        response = s3.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': objects_to_delete}
                        )
                        total_deleted += len(objects_to_delete)
                        
                        # Check for errors
                        if 'Errors' in response and response['Errors']:
                            error_keys = [error['Key'] for error in response['Errors']]
                            flash(f'Some objects could not be deleted: {", ".join(error_keys[:5])}{"..." if len(error_keys) > 5 else ""}', 'warning')
            
            flash(f'Deleted folder and {total_deleted} objects successfully.', 'success')
        else:
            s3.delete_object(Bucket=bucket_name, Key=key)
            flash('File deleted successfully.', 'success')
    except Exception as e:
        flash(f'Error deleting: {e}', 'danger')
    return redirect(url_for('index', prefix='/'.join(key.split('/')[:-1])))

@app.route('/download/<path:key>')
def download_file(key):
    try:
        temp_file_path = 'temp_download_file'
        s3.download_file(bucket_name, key, temp_file_path)
        
        return_value = send_file(
            temp_file_path,
            as_attachment=True,
            download_name=os.path.basename(key)
        )
        
        # Clean up will happen after response is delivered
        @return_value.call_on_close
        def cleanup():
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                
        return return_value
    except Exception as e:
        flash(f'Error downloading file: {e}', 'danger')
        # Clean up temp file in case of error
        if os.path.exists('temp_download_file'):
            os.remove('temp_download_file')
        return redirect(url_for('index', prefix='/'.join(key.split('/')[:-1])))

@app.route('/generate_jibberish', methods=['POST'])
def generate_jibberish():
    prefix = request.form['prefix']
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    try:
        # Create folders
        for _ in range(5):
            folder_name = generate_random_string()
            folder_key = f'{prefix}{folder_name}/'
            s3.put_object(Bucket=bucket_name, Key=folder_key, Body=b'')
        
        # Create files using temporary files
        for _ in range(5):
            file_name = generate_random_string() + '.txt'
            file_content = generate_jibberish_content()
            file_key = f'{prefix}{file_name}'
            
            # Write content to a temporary file
            temp_file = 'temp_jibberish'
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(file_content)
            
            # Upload using the file path
            s3.upload_file(temp_file, bucket_name, file_key)
            
            # Clean up
            os.remove(temp_file)
        
        flash('Random jibberish files and folders created successfully.', 'success')
    except Exception as e:
        flash(f'Error creating jibberish: {e}', 'danger')
        # Clean up temp file in case of error
        if os.path.exists('temp_jibberish'):
            os.remove('temp_jibberish')
    return redirect(url_for('index', prefix=prefix))

@app.route('/cleanup', methods=['POST'])
def cleanup():
    prefix = request.form['prefix']
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        total_deleted = 0
        for page in pages:
            if 'Contents' in page and page['Contents']:
                objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                if objects_to_delete:
                    response = s3.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': objects_to_delete}
                    )
                    total_deleted += len(objects_to_delete)
                    
                    # Check for errors
                    if 'Errors' in response and response['Errors']:
                        error_keys = [error['Key'] for error in response['Errors']]
                        flash(f'Some objects could not be deleted: {", ".join(error_keys[:5])}{"..." if len(error_keys) > 5 else ""}', 'warning')
        
        flash(f'Cleanup successful. Deleted {total_deleted} objects.', 'success')
    except Exception as e:
        flash(f'Error during cleanup: {e}', 'danger')
    return redirect(url_for('index', prefix=prefix))

if __name__ == '__main__':
    app.run(debug=True, port=5001)


from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import boto3
from botocore.config import Config
import urllib3
import random
import string
import os
import io
import re
import hashlib

# Suppress urllib3 warnings for self-signed/internal certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# AWS S3/HCP credentials and endpoint
aws_access_key = 'your-access-key'
aws_secret_access_key = 'your-secret-key'
endpoint_url = 'https://your-hcp-endpoint'  # Update this
bucket_name = 'your-bucket-name'

hcp_config = Config(
    signature_version='s3v4',
    s3={'addressing_style': 'path'}
)

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_access_key,
    endpoint_url=endpoint_url,
    config=hcp_config,
    verify=False  # Set to internal CA path if needed
)

app = Flask(__name__)
app.secret_key = 'supersecretkey'

def list_files_in_folder(prefix):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        folders = [cp['Prefix'] for cp in response.get('CommonPrefixes', [])]
        files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'] != prefix]
        return folders, files
    except Exception as e:
        flash(f'Error listing files: {e}', 'danger')
        return [], []

def generate_random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def generate_jibberish_content():
    return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + ' ', k=100))

def get_breadcrumbs(prefix):
    if not prefix:
        return []
    parts = prefix.strip('/').split('/')
    breadcrumbs = [{'name': 'Home', 'prefix': ''}]
    for i, part in enumerate(parts):
        breadcrumbs.append({'name': part, 'prefix': '/'.join(parts[:i + 1]) + '/'})
    return breadcrumbs

# Function to calculate MD5 hash for S3 Content-MD5 header
def calculate_md5(data):
    if isinstance(data, str):
        data = data.encode('utf-8')
    md5_hash = hashlib.md5(data).digest()
    return md5_hash

@app.route('/')
@app.route('/<path:prefix>')
def index(prefix=''):
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    folders, files = list_files_in_folder(prefix)
    breadcrumbs = get_breadcrumbs(prefix)
    return render_template('index.html', folders=folders, files=files, prefix=prefix, breadcrumbs=breadcrumbs, bucket_name=bucket_name)

@app.route('/create_folder', methods=['POST'])
def create_folder():
    folder_name = request.form.get('folder_name', '').strip()
    prefix = request.form.get('prefix', '').strip()

    if not folder_name:
        flash('Folder name cannot be empty.', 'danger')
        return redirect(url_for('index', prefix=prefix))

    if not re.match(r'^[\w\- ]+$', folder_name):
        flash('Folder name contains invalid characters.', 'danger')
        return redirect(url_for('index', prefix=prefix))

    if prefix and not prefix.endswith('/'):
        prefix += '/'

    new_folder_key = f'{prefix}{folder_name}/'

    try:
        # Use empty string for folder creation
        empty_data = b''
        
        # Get a pre-signed URL for PUT
        url = s3.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': bucket_name,
                'Key': new_folder_key,
                'ContentLength': 0
            },
            ExpiresIn=60
        )
        
        # Use requests to put the object with explicit headers
        import requests
        headers = {
            'Content-Length': '0'
        }
        response = requests.put(url, data=empty_data, headers=headers, verify=False)
        
        if response.status_code in (200, 201):
            flash('Folder created successfully.', 'success')
        else:
            flash(f'Error creating folder: HTTP {response.status_code}', 'danger')
            
    except Exception as e:
        flash(f'Error creating folder: {e}', 'danger')

    return redirect(url_for('index', prefix=prefix))

@app.route('/upload_file', methods=['POST'])
def upload_file():
    file = request.files['file']
    prefix = request.form['prefix']
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    file_key = f'{prefix}{file.filename}'
    
    try:
        # Read file content
        file_content = file.read()
        content_length = len(file_content)
        
        # Get a pre-signed URL for PUT
        url = s3.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': bucket_name,
                'Key': file_key,
                'ContentLength': content_length
            },
            ExpiresIn=60
        )
        
        # Use requests to put the object with explicit headers
        import requests
        headers = {
            'Content-Length': str(content_length)
        }
        response = requests.put(url, data=file_content, headers=headers, verify=False)
        
        if response.status_code in (200, 201):
            flash('File uploaded successfully.', 'success')
        else:
            flash(f'Error uploading file: HTTP {response.status_code}', 'danger')
            
    except Exception as e:
        flash(f'Error uploading file: {e}', 'danger')
    
    return redirect(url_for('index', prefix=prefix))

@app.route('/create_file', methods=['POST'])
def create_file():
    file_name = request.form['file_name']
    file_content = request.form['file_content']
    prefix = request.form['prefix']
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    file_key = f'{prefix}{file_name}'
    
    try:
        # Convert content to bytes
        content_bytes = file_content.encode('utf-8')
        content_length = len(content_bytes)
        
        # Get a pre-signed URL for PUT
        url = s3.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': bucket_name,
                'Key': file_key,
                'ContentLength': content_length
            },
            ExpiresIn=60
        )
        
        # Use requests to put the object with explicit headers
        import requests
        headers = {
            'Content-Length': str(content_length)
        }
        response = requests.put(url, data=content_bytes, headers=headers, verify=False)
        
        if response.status_code in (200, 201):
            flash('File created successfully.', 'success')
        else:
            flash(f'Error creating file: HTTP {response.status_code}', 'danger')
            
    except Exception as e:
        flash(f'Error creating file: {e}', 'danger')
    
    return redirect(url_for('index', prefix=prefix))

@app.route('/edit_file/<path:key>', methods=['GET', 'POST'])
def edit_file(key):
    if request.method == 'POST':
        new_content = request.form['file_content']
        
        try:
            # Convert content to bytes
            content_bytes = new_content.encode('utf-8')
            content_length = len(content_bytes)
            
            # Get a pre-signed URL for PUT
            url = s3.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': key,
                    'ContentLength': content_length
                },
                ExpiresIn=60
            )
            
            # Use requests to put the object with explicit headers
            import requests
            headers = {
                'Content-Length': str(content_length)
            }
            response = requests.put(url, data=content_bytes, headers=headers, verify=False)
            
            if response.status_code in (200, 201):
                flash('File updated successfully.', 'success')
            else:
                flash(f'Error updating file: HTTP {response.status_code}', 'danger')
                
        except Exception as e:
            flash(f'Error updating file: {e}', 'danger')
        
        return redirect(url_for('index', prefix='/'.join(key.split('/')[:-1])))
    else:
        try:
            response = s3.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
        except Exception as e:
            flash(f'Error reading file: {e}', 'danger')
            content = ''
        return render_template('edit.html', key=key, content=content)

@app.route('/delete/<path:key>')
def delete_file_or_folder(key):
    try:
        if key.endswith('/'):
            # Handle folder deletion with pagination and batch delete
            paginator = s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=key)
            
            total_deleted = 0
            for page in pages:
                if 'Contents' in page and page['Contents']:
                    objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                    if objects_to_delete:
                        response = s3.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': objects_to_delete}
                        )
                        total_deleted += len(objects_to_delete)
                        
                        # Check for errors
                        if 'Errors' in response and response['Errors']:
                            error_keys = [error['Key'] for error in response['Errors']]
                            flash(f'Some objects could not be deleted: {", ".join(error_keys[:5])}{"..." if len(error_keys) > 5 else ""}', 'warning')
            
            flash(f'Deleted folder and {total_deleted} objects successfully.', 'success')
        else:
            s3.delete_object(Bucket=bucket_name, Key=key)
            flash('File deleted successfully.', 'success')
    except Exception as e:
        flash(f'Error deleting: {e}', 'danger')
    return redirect(url_for('index', prefix='/'.join(key.split('/')[:-1])))

@app.route('/download/<path:key>')
def download_file(key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=key)
        file_data = response['Body'].read()
        
        return send_file(
            io.BytesIO(file_data),
            mimetype=response.get('ContentType', 'application/octet-stream'),
            as_attachment=True,
            download_name=os.path.basename(key)
        )
    except Exception as e:
        flash(f'Error downloading file: {e}', 'danger')
        return redirect(url_for('index', prefix='/'.join(key.split('/')[:-1])))

@app.route('/generate_jibberish', methods=['POST'])
def generate_jibberish():
    prefix = request.form['prefix']
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    
    import requests
    
    try:
        # Create folders
        for _ in range(5):
            folder_name = generate_random_string()
            folder_key = f'{prefix}{folder_name}/'
            
            # Get a pre-signed URL for PUT
            url = s3.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': folder_key,
                    'ContentLength': 0
                },
                ExpiresIn=60
            )
            
            # Use requests to put the object with explicit headers
            headers = {
                'Content-Length': '0'
            }
            response = requests.put(url, data=b'', headers=headers, verify=False)
            
            if response.status_code not in (200, 201):
                flash(f'Error creating folder: HTTP {response.status_code}', 'warning')
        
        # Create files
        for _ in range(5):
            file_name = generate_random_string() + '.txt'
            file_content = generate_jibberish_content()
            file_key = f'{prefix}{file_name}'
            
            # Convert content to bytes
            content_bytes = file_content.encode('utf-8')
            content_length = len(content_bytes)
            
            # Get a pre-signed URL for PUT
            url = s3.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': file_key,
                    'ContentLength': content_length
                },
                ExpiresIn=60
            )
            
            # Use requests to put the object with explicit headers
            headers = {
                'Content-Length': str(content_length)
            }
            response = requests.put(url, data=content_bytes, headers=headers, verify=False)
            
            if response.status_code not in (200, 201):
                flash(f'Error creating file: HTTP {response.status_code}', 'warning')
        
        flash('Random jibberish files and folders created successfully.', 'success')
    except Exception as e:
        flash(f'Error creating jibberish: {e}', 'danger')
    
    return redirect(url_for('index', prefix=prefix))

@app.route('/cleanup', methods=['POST'])
def cleanup():
    prefix = request.form['prefix']
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        total_deleted = 0
        for page in pages:
            if 'Contents' in page and page['Contents']:
                objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                if objects_to_delete:
                    response = s3.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': objects_to_delete}
                    )
                    total_deleted += len(objects_to_delete)
                    
                    # Check for errors
                    if 'Errors' in response and response['Errors']:
                        error_keys = [error['Key'] for error in response['Errors']]
                        flash(f'Some objects could not be deleted: {", ".join(error_keys[:5])}{"..." if len(error_keys) > 5 else ""}', 'warning')
        
        flash(f'Cleanup successful. Deleted {total_deleted} objects.', 'success')
    except Exception as e:
        flash(f'Error during cleanup: {e}', 'danger')
    return redirect(url_for('index', prefix=prefix))

if __name__ == '__main__':
    app.run(debug=True, port=5001)
