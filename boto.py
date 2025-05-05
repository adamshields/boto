

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
        # Use put_object instead of upload_fileobj
        s3.put_object(
            Bucket=bucket_name,
            Key=new_folder_key,
            Body=''
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
        # Read the file content and get its size
        file_content = file.read()
        
        # Use put_object with the content length
        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=file_content,
            ContentLength=len(file_content)
        )
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
        # Use put_object with explicit content
        content_bytes = file_content.encode('utf-8')
        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=content_bytes,
            ContentLength=len(content_bytes)
        )
        flash('File created successfully.', 'success')
    except Exception as e:
        flash(f'Error creating file: {e}', 'danger')
    return redirect(url_for('index', prefix=prefix))

@app.route('/edit_file/<path:key>', methods=['GET', 'POST'])
def edit_file(key):
    if request.method == 'POST':
        new_content = request.form['file_content']
        try:
            # Use put_object with explicit content length
            content_bytes = new_content.encode('utf-8')
            s3.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=content_bytes,
                ContentLength=len(content_bytes)
            )
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
            folder_key = f'{prefix}{folder_name}/'
            s3.put_object(
                Bucket=bucket_name,
                Key=folder_key,
                Body=''
            )
            
        for _ in range(5):
            file_name = generate_random_string() + '.txt'
            file_content = generate_jibberish_content()
            file_key = f'{prefix}{file_name}'
            
            content_bytes = file_content.encode('utf-8')
            s3.put_object(
                Bucket=bucket_name,
                Key=file_key,
                Body=content_bytes,
                ContentLength=len(content_bytes)
            )
            
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
