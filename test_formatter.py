import pytest
import pandas as pd
from header_list import rename_list, match_list
from WhyMeCSV import df as finished_df

filename = '/Users/derrick/Documents/Random Stuff/CSV/test.csv'
# sep=None so pandas tries to get the delimiter and dtype=str so columns don't sometimes have .0 added
originial_df = pd.read_csv(filename, sep=None, dtype=str, engine='python')

def test_df_not_the_same():
    assert originial_df.values != finished_df.values

# Make sure the columns needed for merger are in the right order
def test_cols_names():
    assert finished_df.columns[0] == 'First Name'
    assert finished_df.columns[1] == 'Last Name'
    assert finished_df.columns[2] == 'Email'

# Check to see if the dtype of columns are object because otherwise sometimes its float 
# and add .0 after numbers when it shouldn't
def test_cols_dtype():
    assert finished_df.dtypes.any() == 'object', "Some columns dtypes are not an object"





if __name__ == '__main__':
    test_df_not_the_same()
    test_cols_names()
    test_cols_dtype()
    print('Passed all test!')
