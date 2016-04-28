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



def process_hdf(filename):

    # with open("/Users/nem41/Documents/apollo/output/test3.txt", "w") as f:

    hdf = pd.HDFStore(filename, "r")

    n = 0
    for c in hdf.select(hdf.keys()[0], where=('infection_state == ["infectious"]'), chunksize=10000, iterator=True):

        l = c.index.names.index('simulator_time')
        l2 = c.index.names.index('age_range_category_label')
        if n == 0:
            r = c.groupby(level=[l, l2]).sum()
        else:
            r = r.add(c.groupby(level=[l, l2]).sum(), fill_value=0)
        n += 1
    # print('took %s seconds to iterate through %d chunks' % (timer(), n))
    #     f.write(str(r.head()) + "\n")

    # i = x.select(x.keys()[0],
    #              "infection_state == ['infectious']", chunksize=10000000)
    # for df in i:
    #     print(df.columns)
    # for row in r.itertuples():
    #     f.write(str(row) + "\n")
    r.to_csv("/Users/nem41/Documents/apollo/output/test.csv", sep=',')

process_hdf('/Users/nem41/Documents/apollo/output/R0.1.4.apollo.h5.04.01.16')