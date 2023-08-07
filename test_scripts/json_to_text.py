import pickle
import bq_stream
import os
import glob

def write():
    schema_files = glob.glob("schema_files\\*.json")

    for file in schema_files:
        cols = bq_stream.load_cols(file)    
        export_path = os.path.split(os.path.splitext(file)[0])[1]

        export_file = open("schema_files\\list_files\\" + export_path , 'ab')
        pickle.dump(cols, export_file)
        export_file.close()

def main():
    file = open("schema_files\\list_files\\Account_schema", 'rb')
    cols = pickle.load(file)
    file.close()

    print(cols[0])
        

if __name__ == "__main__":
    main()