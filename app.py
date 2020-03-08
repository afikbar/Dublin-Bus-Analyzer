from base64 import b64encode, b64decode, standard_b64encode
import os
import json
import requests
import os
import json
from flask import Flask, flash, request, redirect, url_for, send_from_directory
from flask.templating import render_template
from werkzeug.utils import secure_filename
from uuid import uuid1

UPLOAD_FOLDER = 'uploads'
ALLOWED_SCHEMA = ['_id', 'delay', 'congestion', 'lineId', 'vehicleId', 'timestamp', 'areaId', 'areaId1', 'areaId2', 'areaId3', 'gridID', 'actualDelay', 'longitude', 'latitude', 'currentHour', 'dateTypeEnum', 'angle', 'ellapsedTime',
                  'vehicleSpeed', 'distanceCovered', 'journeyPatternId', 'direction', 'busStop', 'poiId', 'poiId2', 'systemTimestamp', 'calendar', 'filteredActualDelay', 'atStop', 'dateType', 'justStopped', 'justLeftStop', 'probability', 'anomaly', 'loc']

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['TEMPLATES_AUTO_RELOAD'] = True

### DBFS ###


TOKEN = b'dapi7b10e95c3a1fd0902c7f89523b3ad76f'
headers = {"Authorization": b"Basic " + standard_b64encode(b"token:" + TOKEN)}
url = "https://eastus.azuredatabricks.net/api/2.0"
dbfs_dir = "dbfs:/FileStore/Afik/DublinApp/Stream/"


def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request(
        'POST', url + path, data=json.dumps(data), verify=True, headers=headers)
    return resp.json()


def mkdirs(path, headers):
    _data = {}
    _data['path'] = path
    return perform_query('/dbfs/mkdirs', headers=headers, data=_data)


def create(path, overwrite, headers):
    _data = {}
    _data['path'] = path
    _data['overwrite'] = overwrite
    return perform_query('/dbfs/create', headers=headers, data=_data)


def add_block(handle, data, headers):
    _data = {}
    _data['handle'] = handle
    _data['data'] = data
    return perform_query('/dbfs/add-block', headers=headers, data=_data)


def close(handle, headers):
    _data = {}
    _data['handle'] = handle
    return perform_query('/dbfs/close', headers=headers, data=_data)


def put_file(src_path, dbfs_path, overwrite, headers):
    handle = create(dbfs_path, overwrite, headers=headers)['handle']
    print("Putting file: " + dbfs_path)
    with open(src_path, 'rb') as local_file:
        while True:
            contents = local_file.read(2**20)
            if len(contents) == 0:
                break
            add_block(handle, b64encode(contents).decode(), headers=headers)
        close(handle, headers=headers)


# mkdirs(path=dbfs_dir, headers=headers)
# resp = put_file(src_path=f, dbfs_path=target_path,
#                 overwrite=True, headers=headers)
# if resp == None:
#     print("Success")
# else:
#     print(resp)

# files = [f for f in os.listdir('.') if os.path.isfile(f)]
# for f in files:
#     if ".png" in f:
#         target_path = dbfs_dir + f
#         resp = put_file(src_path=f, dbfs_path=target_path,
#                         overwrite=True, headers=headers)
#         if resp == None:
#             print("Success")
#         else:
#             print(resp)

#####


def allowed_file(file):
    line = file.readline()
    row_dict = json.loads(line)
    return all(key in row_dict.keys() for key in ALLOWED_SCHEMA)


@app.route('/')
def home():
    return render_template('home.html')

@app.route('/upload')
def upload_form():
    return render_template('upload.html')


@app.route('/map')
def map():
    return render_template('map.html')



@app.route('/upload', methods=['POST'])
def upload_file():
    if request.method == 'POST':
                # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part', 'error')
            return redirect(request.url)
        file = request.files['file']
        # if user does not select file, browser also
        # submit an empty part without filename
        if file.filename == '':
            flash('No selected file', 'error')
            return redirect(request.url)
        try:
            if file and allowed_file(file):
                filename = str(uuid1())
                local_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(local_path)
                flash('File successfully uploaded', 'success')
                # Upload to dbfs:
                print("Uploading to DBFS...")
                resp = put_file(src_path=local_path, dbfs_path=dbfs_dir+filename, overwrite=True, headers=headers)
                if resp == None:
                    print("Successfully upload to DBFS!")
                else:
                    print("Failed to upload: " + resp)
                return redirect('/')
            else:
                flash(f"Allowed Schema is: {ALLOWED_SCHEMA}", 'error')
                return redirect(request.url)
        except Exception as e:
            flash('Something went wrong, please retry', 'error')
            print(f"Error: {e}")
        return redirect(request.url)


# @app.route('/uploads/<filename>')
# def uploaded_file(filename):
#     return send_from_directory(app.config['UPLOAD_FOLDER'], filename)



if __name__ == '__main__':
    app.run()
