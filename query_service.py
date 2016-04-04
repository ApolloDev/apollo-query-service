from flask import Flask, request
import parse_scos_message
import run_query
import urllib.request
import os
import json

app = Flask(__name__)

# files in local files dir should be accessible via urls
LOCAL_FILES_DIR = '/Users/nem41/Documents/apollo/apollo-work-dir/400/query-service-temp/'
FILE_SERVICE_URL = 'http://localhost:8080/file-store-service-rest-frontend-4.0-SNAPSHOT/ws'
RUN_MANAGER_SERVICE_URL = 'http://localhost:8080/run-manager-service-rest-frontend-4.0-SNAPSHOT/ws'


@app.route('/query/<int:run_id>/', methods=['GET'])
def query(run_id):
    username = request.args.get('username')
    password = request.args.get('password')

    try:
        # get scos queries
        # get file from file service

        # list_files_result = urllib.request.urlopen(FILE_SERVICE_URL + '/listfiles').read()
        # find query file
        # query_file_url = urllib.request.urlopen(FILE_SERVICE_URL + '/geturl').read()

        query_file_url = 'http://localhost/symptomatic_by_age_group_in_humans.xml'
        queries = parse_scos_message.get_queries(query_file_url)

        run_dir = LOCAL_FILES_DIR + str(run_id) + '/'
        if not os.path.exists(run_dir):
            os.makedirs(run_dir)

        set_status('RUNNING', 'The query is running', run_id, username, password)

        for i in range(0, len(queries)):
            # get hdf5 file listed in scos
            scos = queries[i]
            file_id = scos['file_id']

            hdf5_file_url = 'http://localhost/fred_out.hdf5' # get url from file store
            local_file = run_dir + str(file_id) + '.hdf5'
            urllib.request.urlretrieve(hdf5_file_url, local_file)

            # run query from scos
            # somehow get the output type
            output_file = run_dir + 'query_output_' + str(file_id) + '.csv'
            run_query.run_query(scos, local_file, output_file)

            # upload file to file store
            data = urllib.parse.urlencode({'username': username, 'password': password})
            binary_data = data.encode('UTF-8')
            urllib.request.urlopen(FILE_SERVICE_URL + '/' + str(run_id), binary_data)

        set_status('COMPLETED', 'The query has completed.', run_id, username, password)

    except Exception as e:
        try:
            set_status('FAILED', 'The query has failed: ' + str(e), run_id, username, password)
        except Exception as e2:
            print('Could not set error status for query run ' + str(run_id) + '. Message was: ' \
                  + str(e2))

    return ''

def set_status(status, message, run_id, username, password):

    # data = urllib.parse.urlencode({'username': username, 'password': password, \
    #                                'statusMessage': message, 'methodCallStatusEnum': status})
    # binary_data = data.encode('UTF-8')
    # urllib.request.urlopen(RUN_MANAGER_SERVICE_URL + '/run/' + str(run_id) + '/status', binary_data)

    print(status + '     ' + message)

if __name__ == "__main__":
    app.run()