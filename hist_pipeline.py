"""Hisotrical Data Pipeline

This script exports a csv file of an object entered through the 
console. The exported csv file contains all historical data for the 
given object with the "As of Date" column (date of each record).

This script accepts comma separated value files (.csv).

The program assumes that the path to the historical data directory is
correct

The historical data folder must contain a subfolder for each year, which
contain subfolders of each Saleforce Export

This script requires that `pandas` be installed within the python
environment you are running this script in.

Also requires that the module data.py is located in the
same folder as the hist_pipeline.py script
"""
import pandas as pd
import os
import glob
import re
import datetime
import data_generator
import numpy as np

def choose_csv_filepaths(path):
    # returns list of filepaths to csv files of object specified
    # through the console, and object name. Pass in path of historical
    # data folder
    while True:
        print("\nEnter object name: ")
        # read object_name through terminal
        object_name = input()
        # Collects filepaths to csv files that match object
        csv_filepaths = glob.glob(os.path.join(path, "*", "*", object_name + ".csv"), 
            recursive=True)

        if len(csv_filepaths) == 0:
            # object not found, rerun with new object
            print("Object \"" + object_name + "\" does not exist in data")
        else:
            break

    return csv_filepaths, object_name

def add_date_col(file_path,p_low_mem, p_engine):
    # Reads csv into dataframe and adds "As_of_Date" column. 
    # Returns a dataframe
    temp_df = pd.read_csv(file_path, encoding="cp1252", engine=p_engine, low_memory=p_low_mem)
    # date from filename in "##.##.##"" format
    temp_date_str = re.search(r"Export (.+?)\\", file_path).group(1)
    # splits date string into list of ["##", "##", "##"]
    temp_date_list = temp_date_str.split(".")

    temp_date = str(datetime.datetime(2000 + int(temp_date_list[2]), int(temp_date_list[0]), int(temp_date_list[1])))

    # temp_df["As_of_Date"] = temp_date
    temp_df.insert(loc=0, column="As_of_Date", value=temp_date)
    return temp_df

def print_export_msgs(object_name, num_rows, json):
    print("\n*****************************")
    print("Successfully exported " + object_name + "_import.csv")
    print("Merged dataframe shape: " + str(num_rows) + "\n")

    if(json):
        print("Successfully exported " + object_name + "_import.json\n")
    print("*****************************")

def reorder_temp_columns(column_list, temp_df):
    temp_col_list = temp_df.columns.tolist()
    # List of columns unique to temp dataframe
    new_cols = np.setdiff1d(temp_col_list, column_list)
    # List of columns unique to csv file
    missing_cols = np.setdiff1d(column_list, temp_col_list)

    for new_col in new_cols:
        column_list.append(new_col)
    for missing_col in missing_cols:
        temp_df[missing_col] = ""
    
    temp_df = temp_df[column_list]
    return column_list, temp_df

def compute_and_export_data(csv_filepaths, export_path, json, json_path=None):
    # Performs add_date_col func on each file
    # Appends dataframe from weekly data to csv file
    # one week at a time
    # Exports a schema of csv file if json=True

    df_list = []
    num_rows = 0
    column_list = []
    p_low_mem = True
    p_engine = 'python'
    generator = data_generator.Schema_generator() if json else None

    print("\nBuilding...")
    for file_path in csv_filepaths:
        try:
            temp_df = add_date_col(file_path, p_low_mem, p_engine)
        except pd.errors.ParserError:
            print("\nCouldn't read: " + file_path)
            print("Building:", end=" ")
            p_low_mem = False
            p_engine = None
            continue
        filename = os.path.split(os.path.split(file_path)[0])[1]
        print('                                            ', end='\r')
        print(filename, end='                        ')
        
        df_list.append(temp_df)

        # List of dataframes for each week of historical data
        num_rows += temp_df.shape[0]

    merged_df = pd.concat(df_list)
    merged_df.to_csv(export_path, index=False, header=True)
    if(json):
        generator.create_schema(merged_df)
    
    print("")
    if json:
        generator.export_json(json_path)

    return num_rows

def main():
    # Path of historical data
    print("Enter path of data folder")
    path = input()
    print("Enter path of export")
    export_path = input()
    # Runs script multiple times
    while True:
        # Creates list of filepaths to objects from each week
        csv_filepaths, object_name = choose_csv_filepaths(path)

        # Prompts y/n to export json file of schema with csv 
        print("\nDo you want to export the schema to a .json file? y/n")
        json = True if input() in ['Y', 'y'] else False

        # Data Pipeline/export
        csv_export = os.path.join(export_path, object_name + ".csv")
        json_export = os.path.join(export_path, object_name + '_schema.json')
        # Exports data to csv file and if requested schema to json
        rows = compute_and_export_data(csv_filepaths, csv_export, json, json_export)
        
        print_export_msgs(object_name, rows, json)

        # Prompts whether to rerun program
        print("Rerun program on new object? y/n:")
        if input() not in ["Y", "y"]:
            break
        print("Do you want to change output path n/[PATH]")
        n_or_path = input()
        if n_or_path not in ['N', 'n']:
            export_path = n_or_path

if __name__ == "__main__":
    main()