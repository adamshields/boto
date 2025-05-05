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
        s3.upload_fileobj(
            Fileobj=io.BytesIO(b''),
            Bucket=bucket_name,
            Key=new_folder_key
        )
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
        s3.upload_fileobj(file, bucket_name, file_key)
        flash('File uploaded successfully.', 'success')
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
        content_bytes = io.BytesIO(file_content.encode('utf-8'))
        s3.upload_fileobj(content_bytes, bucket_name, file_key)
        flash('File created successfully.', 'success')
    except Exception as e:
        flash(f'Error creating file: {e}', 'danger')
    return redirect(url_for('index', prefix=prefix))

@app.route('/edit_file/<path:key>', methods=['GET', 'POST'])
def edit_file(key):
    if request.method == 'POST':
        new_content = request.form['file_content']
        try:
            s3.upload_fileobj(io.BytesIO(new_content.encode('utf-8')), bucket_name, key)
            flash('File updated successfully.', 'success')
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
    try:
        for _ in range(5):
            folder_name = generate_random_string()
            s3.upload_fileobj(io.BytesIO(b''), bucket_name, f'{prefix}{folder_name}/')
        for _ in range(5):
            file_name = generate_random_string() + '.txt'
            file_content = generate_jibberish_content()
            s3.upload_fileobj(io.BytesIO(file_content.encode('utf-8')), bucket_name, f'{prefix}{file_name}')
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

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>S3 File and Folder Management</title>
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css">
</head>
<body>
    <div class="container">
        <h1 class="mt-4">S3 File and Folder Management</h1>
        <h2>Bucket: {{ bucket_name }}</h2>

        <!-- Breadcrumbs -->
        <nav aria-label="breadcrumb">
            <ol class="breadcrumb">
                {% for crumb in breadcrumbs %}
                    <li class="breadcrumb-item {% if loop.last %}active{% endif %}">
                        {% if not loop.last %}
                            <a href="{{ url_for('index', prefix=crumb.prefix) }}">{{ crumb.name }}</a>
                        {% else %}
                            {{ crumb.name }}
                        {% endif %}
                    </li>
                {% endfor %}
            </ol>
        </nav>

        <!-- Create Folder Form -->
        <form action="{{ url_for('create_folder') }}" method="post" class="mb-3">
            <input type="hidden" name="prefix" value="{{ prefix }}">
            <div class="input-group">
                <input type="text" name="folder_name" class="form-control" placeholder="Folder Name" required>
                <div class="input-group-append">
                    <button type="submit" class="btn btn-primary">Create Folder</button>
                </div>
            </div>
        </form>

        <!-- Upload File Form -->
        <form action="{{ url_for('upload_file') }}" method="post" enctype="multipart/form-data" class="mb-3">
            <input type="hidden" name="prefix" value="{{ prefix }}">
            <div class="input-group">
                <input type="file" name="file" class="form-control" required>
                <div class="input-group-append">
                    <button type="submit" class="btn btn-primary">Upload File</button>
                </div>
            </div>
        </form>

        <!-- Create File Form -->
        <form action="{{ url_for('create_file') }}" method="post" class="mb-3">
            <input type="hidden" name="prefix" value="{{ prefix }}">
            <div class="input-group mb-2">
                <input type="text" name="file_name" class="form-control" placeholder="File Name" required>
            </div>
            <div class="input-group mb-2">
                <textarea name="file_content" class="form-control" placeholder="File Content" required></textarea>
            </div>
            <div class="input-group-append">
                <button type="submit" class="btn btn-primary">Create File</button>
            </div>
        </form>

        <!-- Generate Random Jibberish Form -->
        <form action="{{ url_for('generate_jibberish') }}" method="post" class="mb-3">
            <input type="hidden" name="prefix" value="{{ prefix }}">
            <button type="submit" class="btn btn-warning">Generate Random Jibberish</button>
        </form>

        <!-- Cleanup Form -->
        <form action="{{ url_for('cleanup') }}" method="post" class="mb-3">
            <input type="hidden" name="prefix" value="{{ prefix }}">
            <button type="submit" class="btn btn-danger">Cleanup</button>
        </form>

        <!-- Messages -->
        <div id="messages">
            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    <div class="mt-4">
                        {% for category, message in messages %}
                            <div class="alert alert-{{ category }}">{{ message }}</div>
                        {% endfor %}
                    </div>
                {% endif %}
            {% endwith %}
        </div>

        <!-- Files and Folders List -->
        <ul class="list-group mt-4">
            {% for folder in folders %}
                <li class="list-group-item">
                    <a href="{{ url_for('index', prefix=folder) }}">{{ folder }}</a>
                    <a href="{{ url_for('delete_file_or_folder', key=folder) }}" class="btn btn-danger btn-sm ml-2">Delete</a>
                </li>
            {% endfor %}
            {% for file in files %}
                <li class="list-group-item">
                    {{ file }}
                    <a href="{{ url_for('edit_file', key=file) }}" class="btn btn-info btn-sm ml-2">Edit</a>
                    <a href="{{ url_for('download_file', key=file) }}" class="btn btn-success btn-sm ml-2">Download</a>
                    <a href="{{ url_for('delete_file_or_folder', key=file) }}" class="btn btn-danger btn-sm ml-2">Delete</a>
                </li>
            {% endfor %}
        </ul>
    </div>
</body>
</html>


<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Edit File</title>
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css">
</head>
<body>
    <div class="container">
        <h1 class="mt-4">Edit File</h1>
        <h2>{{ key }}</h2>
        
        <form action="{{ url_for('edit_file', key=key) }}" method="post">
            <div class="form-group">
                <textarea name="file_content" class="form-control" rows="20">{{ content }}</textarea>
            </div>
            <div class="form-group">
                <button type="submit" class="btn btn-primary">Save</button>
                <a href="{{ url_for('index', prefix='/'.join(key.split('/')[:-1])) }}" class="btn btn-secondary">Cancel</a>
            </div>
        </form>
    </div>
</body>
</html>
