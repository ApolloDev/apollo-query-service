import sys
print(sys.version)
import pandas as pd
import time
import functools

def row_to_xml(row, file):
    xml = ['<item>']
    for col in row:
        print(col)
    # with open(filename, mode) as f:
    # f.write(res)
    file.write('\n'.join(xml))



def hdf5_to_dataframe(filename):

    # iterator = pd.read_hdf(filename, key=dataset, chunksize=10000)
    # for row in iterator:
    # print (row)

    c = 0
    with open("/Users/nem41/Documents/apollo/output/test.xml", "w") as f:
        row_to_xml_with_file = functools.partial(row_to_xml, file=f)

        x = pd.HDFStore(filename, "r")
        print(x.keys())
        i = x.select(x.keys()[0], chunksize=10000000)
        for df in i:
            for row in df.itertuples():
                f.write(str(row[1])+",")


hdf5_to_dataframe('/Users/nem41/Documents/apollo/output/R0.1.4.apollo.h5.04.01.16')