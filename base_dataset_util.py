import pandas as pd
import h5py

def hdf5_to_dataframe(filename, dataset):

    df = pd.read_hdf(filename, key=dataset)
    return df

def get_row_count(df):
    return len(df.index)