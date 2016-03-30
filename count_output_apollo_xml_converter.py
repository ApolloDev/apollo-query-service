import base_dataset_util
import pandas as pd
import csv
import os
import getopt, sys
import datetime
from functools import partial


def to_xml(df, filename=None, mode='w'):

    def row_to_xml(row, file):
        xml = ['<item>']
        for i, col_name in enumerate(row.index):
            xml.append(' <field name="{0}">{1}</field>'.format(col_name, row.iloc[i]))
            xml.append('</item>')
        # with open(filename, mode) as f:
            # f.write(res)
        file.write('\n'.join(xml))


    with open(filename, mode, 100000000) as f:
        row_to_xml_with_file = partial(row_to_xml, file=f)
        df.apply(row_to_xml_with_file, axis=1)

    # if filename is None:
    #     return res
    # with open(filename, mode) as f:
    #     f.write(res)


def main(argv):
    pd.DataFrame.to_xml = to_xml
    filename = '/Users/nem41/Documents/apollo/output/R0.1.4.apollo.h5'
    # filename = '/Users/nem41/Documents/apollo/output/test.h5'
    # try:
    #   opts, args = getopt.getopt(argv,"hi:o:t:",["ifile="])
    # except getopt.GetoptError:
    #   print('test.py -i <inputfile>')
    # for opt, arg in opts:
    #     if opt == '-h':
    #         print('test.py -i <inputfile> -o <outputlocation>')
    #         sys.exit()
    #     elif opt in ("-i", "--ifile"):
    #         filename = arg
    #     elif opt in("-o", "--olocation"):
    #         output_location = arg
    #     elif opt in("-t", "--tempdir"):
    #         temp_file = arg

    translate_output(filename)

def translate_output(filename):
    # age,gender,race,location,simulator_time,infection_state,disease_state,count
    print("start loading file")
    print(datetime.datetime.now())
    # df = base_dataset_util.hdf5_to_dataframe(filename, 'df/table')
    df = base_dataset_util.hdf5_to_dataframe(filename, 'apollo_aggregated_counts/table')
    print("start printing xml")
    print(datetime.datetime.now())
    # df.to_csv('/Users/nem41/Documents/apollo/output/test2.csv', encoding='utf-8')
    df.to_xml('/Users/nem41/Documents/apollo/output/test.xml')
    print("done")
    print(datetime.datetime.now())
    # df.to_xml('/Users/nem41/Documents/apollo/output/temp.xml')
    # row_iterator = df.iterrows()
    # _, last = row_iterator.next()  # take first item from row_iterator
    # count = 0
    # for index, row in df.iterrows():
    #     count = count + 1
    #     if count % 1000 == 0:
    #         print(count)

if __name__ == "__main__":
   main(sys.argv[1:])

