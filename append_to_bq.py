"""Append to BigQuery 

This script provides functionality to append pandas
frames to the tables in BigQuery

These functions were intended to be called by bq_stream.py

The script requires a json key to be placed in the service_account_key
folder. The key must be active for an enabled service account
The service account must have the  bigquery.user and 
bigquery.dataEditor roles

It also requires that google-cloud-bigquery module is installed
"""

from logging import exception
from google.cloud import bigquery
from venv import create
import pandas as pd
import json

def append_df_to_bq(df, sf_object, schema_dict):
    # Opens dictionary to convert sf object names to names from
    #  bq tables
    dfile = open('salesforce_to_bigquery_dictionary.txt', 'r')
    # TODO add error handling if object not found in dict
    sf_to_bq_dict = json.load(dfile)
    dfile.close()
    # Converts object names
    try:
        bq_object = sf_to_bq_dict[sf_object]
    except:
        print("object not found in dictionary")
        print("See file 'salesforce_to_bigquery_dictionary'")
        raise Exception("Object not found in dictionary")

    # Construct a BigQuery client object.
    client = auth_client()

    bq_schema = create_bq_schema(schema_dict)

    # Sets destination of where to append data
    # *************************************************************************
    table_id = "sfdc-production-warehouse.sfdc_prod_weekly_current." + bq_object
    # *************************************************************************

    add_new_col(client, table_id, bq_schema)

    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )

    load_job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    ) # Make an API request.

    load_job.result()  # Waits for the job to complete.

    # destination_table = client.get_table(table_id)  # Make an API request.
    # print("Loaded {} rows.".format(destination_table.num_rows))


def create_bq_schema(schema_dict):
    # Create a bq schema list from a json schema dictionary

    names = [row['name'] for row in schema_dict]
    types = [row['type'] for row in schema_dict]

    schema = []
    for name, type in zip(names, types):
        schema.append(bigquery.SchemaField(name, type))

    return schema

def add_new_col(client, table_id, bq_schema):
    table = client.get_table(table_id)

    original_schema = table.schema
    table.schema = bq_schema
    table = client.update_table(table, ["schema"])

    if len(table.schema) > len(original_schema):
        print("New column(s) added")


def auth_client():
    # Authenticates a bq client and returns it
    json_key_path = "service_account_key\\sfdc-production-warehouse-4dde683f6858.json"
    return bigquery.Client.from_service_account_json(json_key_path)

def main():
    print("append_to_bq.py script")
    print("designed to be called by bq_stream.py")
    # df = pd.read_csv('C:\\Users\\brady.bogda\\aes_work_22\\data\\test_data\\df_test_data1.csv')
    # append_df_to_bq(df, 'Online_Site__c')

if __name__ == "__main__":
    main()
