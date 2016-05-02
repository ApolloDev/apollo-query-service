from boto.ses.exceptions import SESMaxSendingRateExceededError
from flask import Flask, request
import parse_scos_message
import run_query
import urllib.request
import os
import xml.etree.ElementTree as ET
import _thread as thread

app = Flask(__name__)

# files in local files dir should be accessible via urls
APOLLO_TYPES_NAMESPACE = 'http://types.apollo.pitt.edu/v4_0/'
SERVICES_COMMON_NAMESPACE = 'http://services-common.apollo.pitt.edu/v4_0/'
LOCAL_FILES_DIR = '/Users/nem41/Documents/sites/query-service/'
LOCAL_FILES_URL = 'http://localhost/query-service/'
FILE_SERVICE_URL = 'http://localhost:8080/broker-service-rest-frontend-4.0-SNAPSHOT/ws/files'
RUN_MANAGER_SERVICE_URL = 'http://localhost:8080/broker-service-rest-frontend-4.0-SNAPSHOT/ws'


@app.route('/run/<int:run_id>', methods=['POST'])
def query(run_id):
    username = request.args.get('username')
    password = request.args.get('password')

    try:
        thread.start_new_thread(run_query_thread, (username, password, run_id))
    except:
        return create_response(500)

    return create_response(200)

def run_query_thread(username, password, run_id):
    try:
        # get scos queries
        # get file from file service

        # find query file
        query_file_url = get_scos_file_url(run_id)

        query_container = parse_scos_message.get_queries(query_file_url)
        queries = query_container['queries']
        output_formats = query_container['output_formats']

        run_dir = LOCAL_FILES_DIR + str(run_id) + '/'
        if not os.path.exists(run_dir):
            os.makedirs(run_dir)

        set_status('RUNNING', 'The query is running', run_id, username, password)

        hdf5_file_url = get_hdf5_file_url(run_id)  # get url from file store
        hdf5_file = run_dir + 'simulator_output.hdf5'
        urllib.request.urlretrieve(hdf5_file_url, hdf5_file)

        for i in range(0, len(queries)):
            # get hdf5 file listed in scos
            scos = queries[i]
            file_id = scos['file_id']

            # run query from scos
            files = run_query.run_query(scos, hdf5_file, output_formats, run_dir, file_id)

            # upload file to file store
            for file_container in files:
                local_file_url = LOCAL_FILES_URL + str(run_id) + '/' + file_container['name'];
                file_name = file_container['name']
                file_type = file_container['type']
                file_format = file_container['format']
                data = urllib.parse.urlencode({'username': username, 'password': password, 'urlToFile': local_file_url, \
                                               'fileName': file_name, 'fileType': file_type, 'fileFormat': file_format})
                binary_data = data.encode('UTF-8')
                urllib.request.urlopen(FILE_SERVICE_URL + '/' + str(run_id), binary_data)

        set_status('COMPLETED', 'The query has completed.', run_id, username, password)

    except Exception as e:
        try:
            set_status('FAILED', 'The query has failed: ' + str(e), run_id, username, password)
        except Exception as e2:
            print('Could not set error status for query run ' + str(run_id) + '. Message was: ' \
                  + str(e2))

def get_files_list(run_id):
    list_files_result = urllib.request.urlopen(FILE_SERVICE_URL + '/' + str(run_id)).read()
    tree = ET.fromstring(list_files_result)

    for entry in tree.findall('{' + SERVICES_COMMON_NAMESPACE + '}responseBody'):
        entry_tree = ET.fromstring(entry.text)


def get_output_file_url(run_id, file_label, file_type, file_format):
    response = urllib.request.urlopen(FILE_SERVICE_URL + '/' + str(run_id) + '/url?fileName=' + file_label + '&fileFormat=' + file_format + '&fileType=' + file_type).read()
    tree = ET.fromstring(response)
    for entry in tree.findall('{' + SERVICES_COMMON_NAMESPACE + '}responseBody'):
        file_url = entry.text
        return file_url

def get_scos_file_url(run_id):
    # these will be the properties for the SCOS message xml file
    file_label = 'run_message.xml'
    file_type = 'RUN_MESSAGE'
    file_format = 'TEXT'

    return get_output_file_url(run_id, file_label, file_type, file_format)

    # return 'http://localhost/num_infected_by_location.xml'

def get_hdf5_file_url(run_id):
    # these will be the properties for the hdf5 file
    file_label = 'output.hd5'
    file_type = 'SIMULATOR_LOG_FILE'
    file_format = 'ZIP'

    # return get_output_file_url(run_id, file_label, file_type, file_format)

    return 'http://localhost/R0.1.4.apollo.h5.04.01.16'


def set_status(status, message, run_id, username, password):

    data = urllib.parse.urlencode({'username': username, 'password': password, \
                                   'statusMessage': message, 'methodCallStatusEnum': status})
    binary_data = data.encode('UTF-8')
    urllib.request.urlopen(RUN_MANAGER_SERVICE_URL + '/run/' + str(run_id) + '/status', binary_data)

    print(status + '     ' + message)

def create_response(code):
    ET.register_namespace('common', SERVICES_COMMON_NAMESPACE)

    response = ET.Element('{' + SERVICES_COMMON_NAMESPACE + '}Response')

    response_meta = ET.SubElement(response, '{' + SERVICES_COMMON_NAMESPACE + '}responseMeta')

    status = ET.SubElement(response_meta, 'status')
    status.text = str(code)
    return ET.tostring(response)

if __name__ == "__main__":
    app.run()
    # data = urllib.parse.urlencode({'username': 'apollo_demo', 'password': 'apollo_demo', 'urlToFile': 'http://localhost/query-service/154/query_output_5.csv', \
    #                                'fileName': 'test.csv', 'fileType': 'QUERY_RESULT', 'fileFormat': 'TEXT'})
    # binary_data = data.encode('UTF-8')
    # urllib.request.urlopen(FILE_SERVICE_URL + '/' + str(154), binary_data)


    # ET.register_namespace('common', SERVICES_COMMON_NAMESPACE)
    #
    # response = ET.Element('{' + SERVICES_COMMON_NAMESPACE + '}Response')
    #
    # response_meta = ET.SubElement(response, '{' + SERVICES_COMMON_NAMESPACE + '}responseMeta')
    #
    # status = ET.SubElement(response_meta, 'status')
    # status.text = '200'
