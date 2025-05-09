<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>S3 File and Folder Management</title>
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css">
  <script src="https://unpkg.com/htmx.org@1.9.2"></script>

  <style>
    @keyframes fadeIn {
      from { opacity: 0; }
      to { opacity: 1; }
    }
  
    @keyframes fadeOut {
      from { opacity: 1; }
      to { opacity: 0; }
    }
  
    #search-overlay {
      display: none;
      position: fixed;
      top: 0;
      left: 0;
      z-index: 9999;
      width: 100vw;
      height: 100vh;
      background-color: rgba(0, 0, 0, 0.5);
      justify-content: center;
      align-items: center;
      animation: fadeOut 0.3s ease-out forwards;
      opacity: 0;
    }
  
    form.htmx-request + #search-overlay {
      display: flex;
      animation: fadeIn 0.3s ease-in forwards;
    }
  </style>
  
</head>
<body>
  <div class="container">
    <h1 class="mt-4">S3 File and Folder Management</h1>
    <h2>Bucket: {{ bucket_name }}</h2>
    <p class="text-muted">
        {{ folder_count }} folders — {{ file_count }} files — {{ total_size_hr }} total
      </p>
      
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

    <!-- Search Form -->
    <form id="search-form" hx-get="/search" hx-target="#file-list" class="mb-3">
      <input type="hidden" name="prefix" value="{{ prefix }}">
      <input type="text" name="q" class="form-control" placeholder="Search files/folders">
    </form>

    <!-- Spinner Overlay shown only during HTMX request on the search form -->
    <div id="search-overlay">
      <div class="spinner-border text-light" role="status" style="width: 3rem; height: 3rem;"></div>
      <div class="text-white mt-3">Searching...</div>
    </div>

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
    <div id="file-list">
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
  </div>
</body>
</html>




def list_files_in_folder(prefix):
    folders = []
    files = []
    total_size = 0
    continuation_token = None

    try:
        while True:
            params = {
                'Bucket': bucket_name,
                'Prefix': prefix,
                'Delimiter': '/'
            }
            if continuation_token:
                params['ContinuationToken'] = continuation_token

            response = s3.list_objects_v2(**params)

            if 'CommonPrefixes' in response:
                folders.extend(cp['Prefix'] for cp in response['CommonPrefixes'])

            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'] != prefix:
                        files.append(obj['Key'])
                        total_size += obj['Size']

            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')

    except Exception as e:
        flash(f'Error listing files: {e}', 'danger')

    return folders, files, len(folders), len(files), total_size




def format_bytes(size):
    # Converts bytes to KB, MB, GB, etc.
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"

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
    folders, files, folder_count, file_count, total_size = list_files_in_folder(prefix)
    breadcrumbs = get_breadcrumbs(prefix)
    return render_template(
        'index.html',
        folders=folders,
        files=files,
        prefix=prefix,
        breadcrumbs=breadcrumbs,
        bucket_name=bucket_name,
        file_count=file_count,
        folder_count=folder_count,
        total_size_hr=format_bytes(total_size)
    )



@app.route('/search')
def search():
    time.sleep(15)  # <-- simulate slow search (remove this later)

    prefix = request.args.get('prefix', '')
    query = request.args.get('q', '').lower()

    if prefix and not prefix.endswith('/'):
        prefix += '/'

    folders, files = list_files_in_folder(prefix)

    folders = [f for f in folders if query in f.lower()]
    files = [f for f in files if query in f.lower()]

    return render_template('file_list.html', folders=folders, files=files, prefix=prefix)





@app.route('/create_folder', methods=['POST'])
def create_folder():
    folder_name = request.form['folder_name']
    prefix = request.form['prefix']
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    new_folder = f'{prefix}{folder_name}/'
    try:
        s3.put_object(Bucket=bucket_name, Key=new_folder)
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
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=file)
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
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=file_content)
        flash('File created successfully.', 'success')
    except Exception as e:
        flash(f'Error creating file: {e}', 'danger')
    return redirect(url_for('index', prefix=prefix))

@app.route('/edit_file/<path:key>', methods=['GET', 'POST'])
def edit_file(key):
    if request.method == 'POST':
        new_content = request.form['file_content']
        try:
            s3.put_object(Bucket=bucket_name, Key=key, Body=new_content)
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
        # Check if it's a folder
        if key.endswith('/'):
            # List all objects with this prefix and delete them
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=key)
            if 'Contents' in response:
                for obj in response['Contents']:
                    s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
        else:
            # It's a file, delete it
            s3.delete_object(Bucket=bucket_name, Key=key)
        flash('Deleted successfully.', 'success')
    except Exception as e:
        flash(f'Error deleting: {e}', 'danger')
    return redirect(url_for('index', prefix='/'.join(key.split('/')[:-1])))

@app.route('/download/<path:key>')
def download_file(key):
    try:
        local_path = os.path.join(os.getcwd(), os.path.basename(key))
        s3.download_file(bucket_name, key, local_path)
        return send_file(local_path, as_attachment=True, download_name=os.path.basename(key))
    except Exception as e:
        flash(f'Error downloading file: {e}', 'danger')
        return redirect(url_for('index', prefix='/'.join(key.split('/')[:-1])))

@app.route('/generate_jibberish', methods=['POST'])
def generate_jibberish():
    prefix = request.form['prefix']
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    
    for _ in range(5):  # Create 5 jibberish folders
        folder_name = generate_random_string()
        new_folder = f'{prefix}{folder_name}/'
        try:
            s3.put_object(Bucket=bucket_name, Key=new_folder)
        except Exception as e:
            flash(f'Error creating folder: {e}', 'danger')
    
    for _ in range(5):  # Create 5 jibberish files
        file_name = generate_random_string() + '.txt'
        file_content = generate_jibberish_content()
        file_key = f'{prefix}{file_name}'
        try:
            s3.put_object(Bucket=bucket_name, Key=file_key, Body=file_content)
        except Exception as e:
            flash(f'Error creating file: {e}', 'danger')
    
    flash('Random jibberish files and folders created successfully.', 'success')
    return redirect(url_for('index', prefix=prefix))

@app.route('/cleanup', methods=['POST'])
def cleanup():
    prefix = request.form['prefix']
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
        flash('Cleanup successful.', 'success')
    except Exception as e:
        flash(f'Error during cleanup: {e}', 'danger')
    
    return redirect(url_for('index', prefix=prefix))

if __name__ == '__main__':
    app.run(debug=True, port=5001)

