import xml.etree.ElementTree as ET
import pandas as pd
from parse_scos_message import get_queries_from_scos
import h5py
import os

"""
Author: John Levander

Description:
    The purpose of this program is to extract data from a dataframe using a simulator count output specification as the
    query specification.  This script is in it's early stages of development.  As it sits right now, the program will
    load a dataset of simulator output and filter out all rows that are about LATENT or INFECTIOUS MALES.
Log:
    01/15/2016: Simple example to create dataframe queries given categorical variables.  Next step is to support
                integer variables and age ranges.
    01/19/2016: Started to implement age ranges.  Passing this off to Nick Millett for now.
"""

m_inf = -float('inf')
p_inf = float('inf')

def hdf5_to_dataframe(filename):
    hdf5_file = h5py.File(filename, 'r')

    dataset = list(hdf5_file.keys())[0]
    hdf5_file.close()

    df = pd.read_hdf(filename, key=dataset)
    return df

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def filter_ranges(scos, query):

    column_name = ''
    if 'age_range' in scos["simulator_count_variables"]:
        variable = 'age_range'
        column_name = 'integer_age'
    elif 'household_median_income' in scos["simulator_count_variables"]:
        variable = 'household_median_income'
        column_name = 'household_median_income'
    else:
        return query;

    def filter_ranges_min_max(min, max, query):

        if min == float('-inf') and max == float('inf'):
            # no filtering required
            return query

        if min == float('-inf'):
            new_query = "(" + column_name + " <= " + str(max) + ")"
        elif max == float('inf'):
            new_query = "(" + column_name + " >= " + str(min) + ")"
        else:
            new_query = "(" + column_name + " >= " + str(min) + ") & (" + column_name + " <= " + str(max) + ")"
        query = query + ' and ' + new_query;

        return query

    ranges = scos["simulator_count_variables"][variable]

    for age_range in ranges:
        query = filter_ranges_min_max(ranges[age_range]['range'][0], ranges[age_range]['range'][1], query)

    return query

"""
This function creates queries in the form of: 'b == ["a", "b", "c"]'
The query selects all rows in the dataframe where column b is equal to the value a b or c.
"""
def create_category_query(col_name, vals_to_keep):
    query = col_name + ' == ['
    for val in vals_to_keep:
        # see if val is an integer
        if is_number(val):
            query += val + ","
        else:
            query += "'" + val + "',"
    query = query[:-1]
    query += "]"
    return query

"""
Here we enforce the WHERE clause.  We filter out rows that we do not want, want based on the simulator_count_variables.
For example if the user
only wanted to see the rows that contain data for MALES, it would be specified in the simulator_count_variables, and we
would filter the FEMALES out in this function.
"""
def append_to_query(scos, query):
    #TODO: deal with age_range categories and integer categories
    for simulator_count_variable in scos["simulator_count_variables"]:
        if simulator_count_variable != "age_range" and simulator_count_variable != "household_median_income":
            new_query = create_category_query(simulator_count_variable, scos['simulator_count_variables'][simulator_count_variable])
            if query == '':
                query = new_query
            else:
                query = query + ' and ' + new_query
            # df = df.query(query)
    # return df
    return query


def process_output_options(df, scos):
    output_axes = scos["output_options"]['axes']
    df = df.groupby(list(output_axes))['count'].aggregate(sum)
    df = df.to_frame("count")
    df = df.reset_index()
    return df


def execute_query_hdf(hdf5_file, query, scos):
    hdf = pd.HDFStore(hdf5_file, "r")

    n = 0
    for c in hdf.select(hdf.keys()[0], where=(query), chunksize=10000, iterator=True):

        output_axes = scos["output_options"]['axes']
        axis_indices = []
        for axis in output_axes:
            l = c.index.names.index(axis)
            axis_indices.append(l)
        if n == 0:
            r = c.groupby(level=axis_indices).sum()
        else:
            r = r.add(c.groupby(level=axis_indices).sum(), fill_value=0)
        n += 1

    return r

def execute_query_csv(csv_file, query):
    df = pd.read_csv(csv_file)
    df = df.query(query)
    return df

def print_datasets(dataframe, output_formats, base_directory, file_id):
    files = []
    for output_format in output_formats:
        file_container = {}
        extension = output_format.lower()
        name = str(file_id) + '.' + extension
        output_file = base_directory + '/' + name
        file_container['local_file'] = output_format
        file_container['name'] = name
        file_container['type'] = 'QUERY_RESULT'

        if extension == 'csv':
            dataframe.to_csv(output_file, sep=',', index=False)
            file_container['format'] = 'TEXT'
            files.append(file_container)
        elif extension == 'hdf':
            dataframe.to_hdf(output_file, 'query_results')
            file_container['format'] = 'HDF'
            files.append(file_container)

    return files


def run_query(scos, input_file, output_formats, base_directory, file_id):

    # create single query to apply to dataset
    query = ''
    query = append_to_query(scos, query)
    query = filter_ranges(scos, query)
    print(query)

    filename, file_extension = os.path.splitext(input_file)

    if file_extension == '.h5' or file_extension == '.hdf5' or file_extension == '.hdf':
        # apply the query
        dataframe = execute_query_hdf(input_file, query, scos)
    elif file_extension == '.csv':
        dataframe = execute_query_csv(input_file, query)
        dataframe = process_output_options(dataframe, scos)

    # print the datasets
    files = print_datasets(dataframe, output_formats, base_directory, file_id)
    return files


if __name__ == '__main__':

    tree = ET.parse('/Users/nem41/Documents/sites/filestore-service/5b50c689507e30325ab7d4c59d583116/2/run_message.xml')
    root = tree.getroot()
    query_container = get_queries_from_scos(root)
    queries = query_container['queries']
    output_formats = query_container['output_formats']
    scos = queries[0]
    file_id = scos['file_id']

    run_query(scos, '/Users/nem41/Documents/sites/filestore-service/63bccfae254cd269c2d9b710241e6616/4/385.apollo.csv', output_formats,
              "/Users/nem41/Documents/apollo/output/", file_id)
