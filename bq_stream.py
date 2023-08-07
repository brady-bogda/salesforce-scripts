"""BigQuery Stream

This script streams data from csv files to Google BigQuery Tables

Place a Salesforce Export into the "files_to_import" folder in
the directory

Add the objects that you want to be appended into BigQuery to the 
objects_to_append.txt file

This script outputs a log file to the logs folder in the directory
specificing the successes/failures of each append

This script requires that the load_funcs.py and append_to_bq.py 
modules are in the same directory as the script

This script does not yet account for duplicates. If you get a
success message in the log file, do not rerun the script on the same
backup for the objects that succeeded. The script will appended the 
objects to BigQuery again
"""

import glob
import pandas as pd
import numpy as np
import load_funcs
import append_to_bq
import datetime
import json

def reorder_temp_columns(bq_col_list, csv_df, object):
    """Reorders columns in passed df csv_df to match bq schema

    If csv_df has columns not found in the bq table (from schema), program 
    will return list of new columns, the original df, and False (indicating 
    failure to align columns)

    Otherwise program will return the list of columns in the bq table, 
    the reordered df, and True (indicating successful reordering)

    If the csv is missing columns from the bq table, null columns will be
    added to the df
    """
    csv_col_list = csv_df.columns.tolist()
    # List of columns unique to csv dataframe
    new_cols = np.setdiff1d(csv_col_list, bq_col_list)
    # List of columns unique to bq table
    missing_cols = np.setdiff1d(bq_col_list, csv_col_list)

    if len(new_cols) > 0: 
        schema = update_schema(csv_df, new_cols, object)
    else:
        schema = load_funcs.load_schema(object)

    # Adds missing columns from bq to csv df
    empty_df = pd.DataFrame(columns=missing_cols)
    csv_df = pd.concat([csv_df, empty_df], axis=1)
    
    new_cols = load_funcs.load_cols(schema)

    reordered_df = csv_df[new_cols]
    return reordered_df, schema

def add_date_col(file_path,p_low_mem=True, p_engine='python'):
    # Reads csv into dataframe and adds "As_of_Date" column. 
    # Returns a dataframe
    temp_df = pd.read_csv(file_path, encoding="cp1252", engine=p_engine, low_memory=p_low_mem)

    temp_date = load_funcs.get_export_date(file_path)

    # temp_df["As_of_Date"] = temp_date
    temp_df.insert(loc=0, column="As_of_Date", value=temp_date)
    return temp_df, temp_date

def update_schema(df, new_cols, object):
    # Updates schema file for specified object and returns new schema
    # Pass in df to identify types and list of new_cols to be added
    schema_dict = load_funcs.load_schema(object)

    type_dict = {
            "b" : "BOOLEAN",
            "i" : "INTEGER",
            "f" : "FLOAT",
            "O" : "STRING",
            "S" : "STRING",
            "U" : "STRING",
            "M" : "TIMESTAMP"
        }

    for col in new_cols:
        schema_dict.append({"name": col, "type": type_dict.get(df.dtypes[col].kind, "STRING")})
        print(col + ": " + type_dict.get(df.dtypes[col].kind, "STRING"))

    with open("schema_files\\" + object + "_schema.json", 'w', encoding='utf-8') as jsonfile:
        json.dump(schema_dict, jsonfile, ensure_ascii=False, indent=4)

    return schema_dict

def send_csv_to_bq(path, object, of):
    # Loads columns from bq database, from json file
    of.write("Appending to " + object + "\n")
    cols = load_funcs.load_cols(load_funcs.load_schema(object))
    obj_df, date = add_date_col(path)
    reordered_df, schema = reorder_temp_columns(cols, obj_df, object)
    try:
        append_to_bq.append_df_to_bq(reordered_df, object, schema)
        of.write("Successfully appended week " + date +"\n")
        of.write("Dataframe Size: " + str(reordered_df.shape) + '\n')
    except Exception as e:
        print(e)
        bq_error_msgs(object, date, of)


def bq_error_msgs(object, date, of):
    of.write("----------------------------------------\n")
    of.write("ERROR: FAILED WEEK " + date + " to " + object + " object\n")
    of.write("Could not append to BigQuery Table\n")
    of.write("Check error messages in terminal\n")
    of.write("----------------------------------------\n")

def main():
    log_date = datetime.datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")
    of = open('logs\\' + str(log_date) + '.txt', 'w')
    # Gets paths of all objects in backup folder
    # TODO get paths from the sf_objects_to_append list to speed up
    paths = glob.glob("files_to_import\\Salesforce Export *\\*.csv")

    if(len(paths) == 0):
        of.write("CSV files not found\n")
        of.close()
        return 

    # Only append these big query tables
    # Object names from Saleforce
    objects_file = open('objects_to_append.txt', 'r')
    sf_object_to_append = objects_file.read().splitlines()
    objects_file.close()
    # Object names from big query

    for path in paths:
        # if object in list to append
        object = load_funcs.get_object_name(path)
        if object in sf_object_to_append:
            of.write("***********************************\n")
            send_csv_to_bq(path, object, of)
            of.write("***********************************\n")
    of.close()


if __name__ == "__main__":
    main()