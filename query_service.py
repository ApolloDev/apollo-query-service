from flask import Flask, request
import parse_scos_message
import run_query
import urllib.request
import os
import json


app = Flask(__name__)

LOCAL_FILES_DIR = '/Users/nem41/Documents/apollo/apollo-work-dir/400/query-service-temp/'

@app.route('/query/<int:run_id>/', methods=['GET'])
def query(run_id):
    # username = request.args.get('username')
    # password = request.args.get('password')

    # get scos queries
    # get file from file service
    url = 'http://localhost/symptomatic_by_age_group_in_humans.xml'
    queries = parse_scos_message.get_queries(url)

    run_dir = LOCAL_FILES_DIR + str(run_id) + '/'
    if not os.path.exists(run_dir):
        os.makedirs(run_dir)

    # make status file
    running_file_path = run_dir + 'running.txt'
    running_file = open(running_file_path, 'w')
    running_file.close()

    for i in range(0, len(queries)):
        # get hdf5 file listed in scos
        scos = queries[i]
        file_id = scos['file_id']

        hdf5_file_url = 'http://localhost/fred_out.hdf5' # get url from file store
        local_file = run_dir + str(file_id) + '.hdf5'
        urllib.request.urlretrieve(hdf5_file_url, local_file)

        # run query from scos
        output_file = run_dir + 'output_' + str(i) + '.csv'
        run_query.run_query(scos, local_file, output_file)

    # make completed file
    completed_file_path = run_dir + 'completed.txt'
    completed_file = open(completed_file_path, 'w')
    completed_file.close()

    return ''

@app.route('/query/<int:run_id>/status', methods=['GET'])
def status(run_id):

    run_dir = LOCAL_FILES_DIR + str(run_id) + '/'

    status = {}

    if os.path.isfile(run_dir + 'error.txt'):
        with open(run_dir + 'error.txt', 'r') as error_file:
            content = error_file.read()
            status['status'] = 'FAILED'
            status['message'] = content
    else:
        if os.path.isfile(run_dir + 'completed.txt'):
            status['status'] = 'COMPLETED'
            status['message'] = 'The query has completed'
        else:
            status['status'] = 'RUNNING'
            status['message'] = 'The query is running'

    return json.dumps(status)

if __name__ == "__main__":
    app.run()