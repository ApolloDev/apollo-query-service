import sys
print(sys.version)
import pandas as pd
import datetime
import functools

def row_to_xml(row, file):
    xml = ['<item>']
    for col in row:
        print(col)
    # with open(filename, mode) as f:
    # f.write(res)
    file.write('\n'.join(xml))



def process_hdf(filename):

    print(datetime.datetime.now())

    hdf = pd.HDFStore(filename, "r")

    n = 0
    for c in hdf.select(hdf.keys()[0], where=('infection_state == ["infectious", "latent", "susceptible", "recovered"]'), chunksize=10000, iterator=True):

        l = c.index.names.index('simulator_time')
        l2 = c.index.names.index('infection_state')
        if n == 0:
            r = c.groupby(level=[l, l2]).sum()
        else:
            r = r.add(c.groupby(level=[l, l2]).sum(), fill_value=0)
        n += 1

    r.to_csv("/Users/nem41/Documents/apollo/output/test2.csv", sep=',')
    print(datetime.datetime.now())


process_hdf('/Users/nem41/Documents/apollo/output/R0.1.4.apollo.h5.04.01.16')