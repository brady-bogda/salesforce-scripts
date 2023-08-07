from tabnanny import verbose
import pandas as pd
import datetime
import pickle
import sys

sys.path.append('..')

import Schema_generator


def store_data():
    df = pd.read_csv(
        r'C:\Users\brady.bogda\aes_work_22\script\output_data\Account_10_week_TEST.csv',
        engine='python')

    dbfile = open('df_pickle', 'ab')
    pickle.dump(df, dbfile)
    dbfile.close()

def load_data():
    dbfile = open('df_pickle', 'rb')
    df = pickle.load(dbfile)
    dbfile.close()
    return df

def test():
    df = load_data()
    # print(df.head(5))
    # print(df.info(verbose=True))
    # df_date = df.apply(pd.to_datetime(df.columns, errors='ignore'))


    # ****WORKING CODE SEGMENT *******
    # converts dates in a dataframe to datetime objects
    for col, dtype in zip(df.columns, df.dtypes):
        if dtype == 'object':
            df[col] = pd.to_datetime(df[col], errors='ignore', format='%Y-%m-%d %H:%M:%S')

    print(df.info(verbose=True, show_counts=True))
    # print(df.head(5))


    # df["CreatedDate"] = pd.to_datetime(df['CreatedDate'], errors='ignore', format='%Y-%m-%d %H:%M:%S')
    # print(df['CreatedDate'].info(verbose=True))
    # print(df['CreatedDate'].head(5))

def test2():
    # print(datetime.datetime(2020, 3, 13))
    json_test = [
    { "name": "Id", "type": "STRING"}, {"name": "IsDeleted", "type": "INTEGER"},
    {"name": "As_of_Date", "type": "object"}]
    
    # Code to reassing AS of Date type to DATE 
    index = [name_i for name_i, type_i in enumerate(json_test) if 'As_of_Date' in type_i.values()]
    json_test[index[0]]['type'] = "DATE"


    print(index[0])
    print(json_test[2]['type'])

    print(json_test[2]['type'])

def test3():
    df = load_data()

    # print(df.info(verbose=True, show_counts=True))

    Schema_generator.export_null_lists(df, 'Account')


if __name__ == '__main__':
    # store_data()
    # test()
    # test2()
    test3()