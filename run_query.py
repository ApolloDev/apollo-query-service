import xml.etree.ElementTree as ET
import pandas as pd
from parse_scos_message import get_queries_from_scos

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


# Sample Simulator Count Output Specifications
all_sick_scos = {}
all_sick_scos["simulator_count_variables"] = {}
all_sick_scos["simulator_count_variables"]['infection_state'] = ["LATENT", "INFECTIOUS"]
all_sick_scos["output_options"] = {}
all_sick_scos["output_options"]['axes'] = {'simulator_time'}


all_sick_by_sex = {}
all_sick_by_sex["simulator_count_variables"] = {}
all_sick_by_sex["simulator_count_variables"]['infection_state'] = ["LATENT", "INFECTIOUS"]
all_sick_by_sex["simulator_count_variables"]['sex'] = ["M"]

all_sick_by_sex_by_age_range = {}
#this is the where clause
all_sick_by_sex_by_age_range["simulator_count_variables"] = {}
all_sick_by_sex_by_age_range["simulator_count_variables"]['infection_state'] = ["LATENT", "INFECTIOUS"]
all_sick_by_sex_by_age_range["simulator_count_variables"]['sex'] = ["M", "F"]
all_sick_by_sex_by_age_range["simulator_count_variables"]['age_range'] = {}
all_sick_by_sex_by_age_range["simulator_count_variables"]['age_range']['driving_age'] = [16,999]
all_sick_by_sex_by_age_range["simulator_count_variables"]['age_range']['voting_age'] = [18,999]
all_sick_by_sex_by_age_range["simulator_count_variables"]['age_range']['retirement_age'] = [65,999]
#this is the select clause
all_sick_by_sex_by_age_range["output_options"] = {}
all_sick_by_sex_by_age_range["output_options"]['axes'] = {'simulator_time', 'age_range'}

all_sick_by_sex_by_age = {}
#this is the where clause
all_sick_by_sex_by_age["simulator_count_variables"] = {}
all_sick_by_sex_by_age["simulator_count_variables"]['infection_state'] = ["LATENT", "INFECTIOUS"]
all_sick_by_sex_by_age["simulator_count_variables"]['sex'] = ["M", "F"]
all_sick_by_sex_by_age["simulator_count_variables"]['integer_age'] = {"min_age" : 30, "max_age" : 40}
#this is the select clause
all_sick_by_sex_by_age["output_options"] = {}
all_sick_by_sex_by_age["output_options"]['axes'] = {'simulator_time', 'integer_age'}

"""
TODO: Add support for these in next version.
all_sick_by_sex_by_age = {}
all_sick_by_sex_by_age["simulator_count_variables"] = {}
all_sick_by_sex_by_age["simulator_count_variables"]['infection_states'] = ["LATENT", "INFECTIOUS"]
all_sick_by_sex_by_age["simulator_count_variables"]['sex'] = ["M", "F"]
all_sick_by_sex_by_age["simulator_count_variables"]['integer_age'] = True



all_sick_by_sex_by_age_state = {}
all_sick_by_sex_by_age_state["simulator_count_variables"] = {}
all_sick_by_sex_by_age_state["simulator_count_variables"]['infection_state'] = ["LATENT", "INFECTIOUS"]
all_sick_by_sex_by_age_state["simulator_count_variables"]['sex'] = ["M", "F"]
all_sick_by_sex_by_age_state["simulator_count_variables"]['integer_age'] = "all"
all_sick_by_sex_by_age_state["simulator_count_variables"]['location_admin1'] = "all"
"""

m_inf = -float('inf')
p_inf = float('inf')

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def filter_ranges(scos, df):

    column_name = ''
    if 'age_range' in scos["simulator_count_variables"]:
        variable = 'age_range'
        column_name = 'integer_age'
    elif 'household_median_income' in scos["simulator_count_variables"]:
        variable = 'household_median_income'
        column_name = 'household_median_income'
    else:
        return df;

    def filter_ranges_min_max(df, min, max):

        if min == float('-inf') and max == float('inf'):
            # no filtering required
            return df

        if min == float('-inf'):
            query = "(" + column_name + " <= " + str(max) + ")"
        elif max == float('inf'):
            query = "(" + column_name + " >= " + str(min) + ")"
        else:
            query = "(" + column_name + " >= " + str(min) + ") & (" + column_name + " <= " + str(max) + ")"

        # remove rows with age below min or above max
        df = df.query(query)
        return df

    ranges = scos["simulator_count_variables"][variable]


    # get min and max from all ranges
    min = float("inf")
    max = -float("inf")
    for age_range in ranges:
        if ranges[age_range]['range'][0] < min:
            min = ranges[age_range]['range'][0]

        if ranges[age_range]['range'][1] > max:
            max = ranges[age_range]['range'][1]


    # filter out all ages outside min and max
    df = filter_ranges_min_max(df, min, max)

    count = 0
    for age_range in ranges:
        print (age_range + " bin is " + str(ranges[age_range]['range'][0]) + " to " + str(ranges[age_range]['range'][1]))
        # get copy of dataframe to filter
        dfcopy = df.copy()
        dfcopy = filter_ranges_min_max(dfcopy, ranges[age_range]['range'][0], ranges[age_range]['range'][1])
        # add the age range column to the data frame
        dfcopy['age_range'] = age_range
        if count == 0:
            newdf = dfcopy
        else:
            newdf = pd.concat([newdf, dfcopy], axis=0)
        count = count + 1

    return newdf

"""
This function creates queries in the form of: 'b == ["a", "b", "c"]'
The query selects all rows in the dataframe where column b is equal to the value a b or c.
"""
def create_category_query(df, col_name, vals_to_keep):
    query = df.columns[df.columns.get_loc(col_name)] + ' == ['
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
def filter_df(df, scos):
    #TODO: deal with age_range categories and integer categories
    for simulator_count_variable in scos["simulator_count_variables"]:
        if simulator_count_variable != "age_range" and simulator_count_variable != "household_median_income":
            query = create_category_query(df, simulator_count_variable, scos['simulator_count_variables'][simulator_count_variable])
            df = df.query(query)
    return df


def process_output_options(df, scos):
    output_axes = scos["output_options"]['axes']
    df = df.groupby(list(output_axes))['count'].aggregate(sum)
    df = df.to_frame("count")
    df = df.reset_index()
    return df


def run_query(scos, hdf5_file):

    file_id = scos['file_id']

    #load the simulator output into a dataframe
    line_listing = pd.read_csv('http://research.rods.pitt.edu/line_listing_new.csv')

    # line_listing['species'] = line_listing['species'].astype(str)

    #Filter out rows from the dataframe that we don't want
    df = filter_df(line_listing, scos)

    df = filter_ranges(scos, df)

    df = process_output_options(df, scos)

    df.to_csv('/Users/nem41/Documents/apollo/python_output_test.csv', sep=',', index=False)

if __name__ == '__main__':

    tree = ET.parse('/Users/nem41/Documents/code_projects/apollo_projects/example-scos-messages/symptomatic_by_age_group_in_humans.xml')
    root = tree.getroot()
    queries = get_queries_from_scos(root)

    run_query(queries[0], '')
