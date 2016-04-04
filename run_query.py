import xml.etree.ElementTree as ET
import pandas as pd
from parse_scos_message import get_queries_from_scos
import h5py

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

    def filter_ranges_min_max(df, min, max, query):

        if min == float('-inf') and max == float('inf'):
            # no filtering required
            return df

        if min == float('-inf'):
            new_query = "(" + column_name + " <= " + str(max) + ")"
        elif max == float('inf'):
            new_query = "(" + column_name + " >= " + str(min) + ")"
        else:
            new_query = "(" + column_name + " >= " + str(min) + ") & (" + column_name + " <= " + str(max) + ")"
        query = query + ' and ' + new_query;

        # remove rows with age below min or above max
        df = df.query(query)
        return df

    ranges = scos["simulator_count_variables"][variable]


    # get min and max from all ranges
    # min = float("inf")
    # max = -float("inf")
    # for age_range in ranges:
    #     if ranges[age_range]['range'][0] < min:
    #         min = ranges[age_range]['range'][0]
    #
    #     if ranges[age_range]['range'][1] > max:
    #         max = ranges[age_range]['range'][1]
    #
    #
    # # filter out all ages outside min and max
    # df = filter_ranges_min_max(df, min, max)

    # count = 0
    for age_range in ranges:
        # print (age_range + " bin is " + str(ranges[age_range]['range'][0]) + " to " + str(ranges[age_range]['range'][1]))
        # get copy of dataframe to filter
        # dfcopy = df.copy()
        query = filter_ranges_min_max(ranges[age_range]['range'][0], ranges[age_range]['range'][1], query)
        # add the age range column to the data frame
        # dfcopy['age_range'] = age_range
        # if count == 0:
        #     newdf = dfcopy
        # else:
        #     newdf = pd.concat([newdf, dfcopy], axis=0)
        # count = count + 1

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


def execute_query(hdf5_file, query):
    with open("/Users/nem41/Documents/apollo/output/query_test.xml", "w") as f:

        x = pd.HDFStore(hdf5_file, "r")
        print(x.keys())
        i = x.select(x.keys()[0], query, chunksize=10000000)
        for df in i:
            for row in df.itertuples():
                f.write(str(row) + "\n")

def run_query(scos, hdf5_file, output_file):

    # create single query to apply to dataset
    query = ''
    query = append_to_query(scos, query)
    query = filter_ranges(scos, query)
    print(query)

    # apply the query
    # execute_query(hdf5_file, query)

    # df = process_output_options(df, scos)

    # df.to_csv(output_file, sep=',', index=False)

if __name__ == '__main__':

    tree = ET.parse('/Users/nem41/Documents/code_projects/apollo_projects/example-scos-messages/num_infected_by_location.xml')
    root = tree.getroot()
    queries = get_queries_from_scos(root)

    run_query(queries[0], '/Users/nem41/Documents/apollo/output/R0.1.4.apollo.h5.04.01.16', '/Users/nem41/Documents/apollo/output/query_test.csv')
