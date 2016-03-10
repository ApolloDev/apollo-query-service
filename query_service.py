from flask import Flask, request
import parse_scos_message
import run_query
import urllib.request
import os


app = Flask(__name__)

LOCAL_FILES_DIR = '/Users/nem41/Documents/apollo/apollo-work-dir/400/query-service-temp/'

@app.route('/query/<int:run_id>/', methods=['GET'])
def query(run_id):
    # username = request.args.get('username')
    # password = request.args.get('password')

    # get scos queries
    url = 'http://localhost/symptomatic_by_age_group_in_humans.xml'
    queries = parse_scos_message.get_queries(url)

    for i in range(0, len(queries)):
        # get hdf5 file listed in scos
        scos = queries[i]
        file_id = scos['file_id']

        hdf5_file_url = 'http://localhost/fred_out.hdf5' # get url from file store
        run_dir = LOCAL_FILES_DIR + str(run_id) + '/'
        if not os.path.exists(run_dir):
            os.makedirs(run_dir)
        local_file = run_dir + str(file_id) + '.hdf5'
        urllib.request.urlretrieve(hdf5_file_url, local_file)

        # run query from scos
        run_query.run_query(scos, local_file)

    return ''

if __name__ == "__main__":
    app.run()