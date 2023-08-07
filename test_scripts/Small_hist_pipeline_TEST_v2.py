"""Hisotrical Data Pipeline **Small Scale Version 2**

This script exports a csv file of an object entered through the 
console. The exported csv file contains **10 Weeks** of historical data for the 
given object with the "As of Date" column (date of each record).

This script accepts comma separated value files (.csv).

The program assumes that the path to the historical data directory is
correct

The historical data folder must contain a subfolder for each year, which
contain subfolders of each Saleforce Export

This script requires that `pandas` be installed within the python
environment you are running this script in.
"""

import pandas as pd;
import os
import glob
import re
import datetime
import Schema_generator

def choose_csv_filepaths(path):
    # returns list of filepaths to csv files of object specified
    # through the console, and object name. Pass in path of historical
    # data folder
    while True:
        print("\nEnter object name: ")
        # read object_name through terminal
        object_name = input()
        csv_filepaths = glob.glob(os.path.join(path, "*", "*", object_name + ".csv"), 
            recursive=True)

        if len(csv_filepaths) == 0:
            # object not found, rerun with new object
            print("Object \"" + object_name + "\" does not exist in data")
        else:
            break

    return csv_filepaths, object_name

def add_date_col(file_path):
    # Reads csv into dataframe and adds "As of Date" column. 
    # Returns a dataframe

    # temp_df = pd.read_csv(file_path, encoding="cp1252", engine="python")
    #this statement needs to Task object
    temp_df = pd.read_csv(file_path, encoding="cp1252", low_memory=False)
    # date from filename in "##.##.##"" format
    temp_date_str = re.search(r"Export (.+?)\\", file_path).group(1)
    # splits date string into list of ["##", "##", "##"]
    temp_date_list = temp_date_str.split(".")

    temp_date = datetime.date(2000 + int(temp_date_list[2]), int(temp_date_list[0]), int(temp_date_list[1]))

    temp_df["As_of_Date"] = temp_date
    return temp_df

def print_export_msgs(object_name, merged_df, json):
    print("\n*****************************")
    print("Successfully exported " + object_name + "_import.csv")
    print("Merged dataframe shape: " + str(merged_df.shape) + "\n")

    if(json):
        print("Successfully exported " + object_name + "_import.json\n")
    print("*****************************")

def main():
    # Path of historical data
    print("Enter path of data folder")
    path = input()
    # Runs script multiple times
    while True:
        csv_filepaths, object_name = choose_csv_filepaths(path)

        print("\nDo you want to export the schema to a .json file? y/n")
        json = True if input() in ['Y', 'y'] else False

        print("\nBuilding:", end=" ")
        df_list = []
        # weeks = [0, 10]
        length = len(csv_filepaths)
        # 5 week list: used with export_null_lists
        weeks = [length-50, length-30, length-3, length-2, length-1]
        animation = "|/-\\"
        idx = 0
        for week in weeks:
            try:
                temp_df = add_date_col(csv_filepaths[week])
            except:
                print("Couldn't read: " + csv_filepaths[week])
                continue
            # List of dataframes for each week of historical data
            df_list.append(temp_df)
            #print("Added dataframe with shape " + str(temp_df.shape))
            print(animation[idx % len(animation)], end="\rBuilding: ")
            idx += 1

        merged_df = pd.concat(df_list, axis=0)

        # Used for getting a txt and csv file of columns
        # with 80% of values missing
        Schema_generator.export_null_lists(merged_df, object_name)


        print("*Build Complete*\nExporting...", end="")

        merged_df.to_csv(os.path.join(os.getcwd(), "output_data", object_name + "_to_import_SMALL_TEST.csv"), index=False)
        print("")
        if(json):
            Schema_generator.export_json(merged_df, object_name)
        
        print_export_msgs(object_name, merged_df, json)


        print("Rerun program on new object? y/n:")
        if input() not in ["Y", "y"]:
            break

if __name__ == "__main__":
    main()