"""Load functions

These helper functions are to be called in bq_stream.py
"""

import json
import os
import re
import datetime

def load_cols(schema_dict):
    # Returns a list of columns from a schema dictionary
    return [row['name'] for row in schema_dict]

def load_schema(object):
    # Returns schema dictionary from json file, from object name passed in
    jsonfile = open("schema_files\\" + object + "_schema.json", 'r')
    schema_dict = json.load(jsonfile)
    jsonfile.close()

    return schema_dict

def get_object_name(path):
    # Returns the object name from a given path
    return os.path.split(os.path.splitext(path)[0])[1]

def get_export_date(path):
    # Returns the date from filename in "yyyy-mm-dd H:M:S"" format
    temp_date_str = re.search(r"Export (.+?)\\", path).group(1)
    # splits date string into list of ["##", "##", "##"]
    temp_date_list = temp_date_str.split(".")
    temp_date = str(datetime.datetime(2000 + int(temp_date_list[2]), 
                    int(temp_date_list[0]), int(temp_date_list[1])))

    return temp_date