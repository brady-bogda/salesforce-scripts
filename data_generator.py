"""Data Module

This script generates a JSON schema from a csv file. 

The csv file path is passed by the user. 

This script requires that `pandas` is installed within the python
environment that is running the script

This file can be imported as a module and contains the following 
functions: 

* exportjson - exports a json of the schema to output data folder
"""

import pandas as pd
import json
import os
class Schema_generator:
    def __init__(self):
        None
        self.schema = []
        self.col_count = 0

    def convert_obj_to_date(self, df):
        # Converts date columns in df from object dtype to datetime
        for col, dtype in \
            zip(df.columns[self.col_count:], df.dtypes[self.col_count:]):
            if dtype == 'object':
                df[col] = pd.to_datetime(df[col], errors='ignore', format='%Y-%m-%d %H:%M:%S')
        return df

    def export_json(self, path):
        """Exports json file to specified path

        Parameters
        ----------
        path : str
            path for json file export
        
        """
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(self.schema, file, ensure_ascii=False, indent=4)

    def create_schema(self, temp_df):
        """Creates schema from temp_df dataframe

        Can be called multiple times on the same dataframe as columns
        are added

        Parameters
        ----------
        df : pd.dataframe
            the df to generate schema of       
        """
        type_dict = {
            "b" : "BOOLEAN",
            "i" : "INTEGER",
            "f" : "FLOAT",
            "O" : "STRING",
            "S" : "STRING",
            "U" : "STRING",
            "M" : "TIMESTAMP"
        }

        df = self.convert_obj_to_date(temp_df)

        self.schema += [{"name" : col_name,
                            "type" : type_dict.get(col_type.kind, "STRING")} 
            for (col_name, col_type) in df.dtypes[self.col_count:].iteritems()]

        # if self.col_count == 0:
            #messy way to change As_of_Date schema type to DATE
            # index = [name_i for name_i, type_i in enumerate(self.schema) if 'As_of_Date' in type_i.values()]
            # self.schema[index[0]]['type'] = "DATE"

        self.col_count = temp_df.shape[1]


def export_null_lists(df, object_name):
    """Exports csv and txt file containing files that are missing 
    more than 80% of value

    Parameters
    ----------
    df : pd.dataframe
        the df to generate files of
    object_name : str
        name of object associated with columns 
    
    """
    #computes total missing for each col
    s = pd.DataFrame(df.isna().sum()).reset_index()
    s.columns = [object_name + '_column_names', 'number_of_missing_values']
    count = df['Id'].count()
    #computes percent missing for each col
    s['percent_missing'] = (s['number_of_missing_values']/count * 100).astype(int)
    missing_80_df = s.loc[s.percent_missing >= 80]

    #saves columns and extra information to csv file
    path_csv = ("C:\\Users\\brady.bogda\\aes_work_22\\script\\data\\empty_columns_data\\"
                + object_name + "_na_cols_data.csv")
    missing_80_df.to_csv(path_csv, index=False)

    #save columns names to txt file
    path_txt = ("C:\\Users\\brady.bogda\\aes_work_22\\script\\data\\empty_columns_data\\" 
        + object_name + "_na_columns.txt")
    missing_80_df[object_name + '_column_names'].to_csv(path_txt, index=False)
    # file = open(path_txt, 'w+')
    # file.dump(missing_80_df[object_name + '_column_names'], file)
    # file.close()

def main():
    print("Enter path of file")
    path = input()
    try:
        df = pd.read_csv(path, engine='python')
    except FileNotFoundError:
        print("Could not find file at entered path: ")
        print(path)
        return
    except: 
        print("Could not read csv file")
        return

    # Removes .csv from file path
    dir_of_csv = os.path.splitext(path)[0]
    # Returns name of zip file
    object_name = "\\".join(dir_of_csv.strip("\\").split("\\")[1:])

    # export_json(df, object_name)



if __name__ == "__main__":
    main()