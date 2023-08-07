""" Data Unzipping Script

This script takes in a path of the compressed data, and the path to 
extract the data, and will unzip all the data from the compressed data
path to the extracted data path

Program assumes that the path to data is correct

Program assumes path to the extracted data folder exists
"""


import zipfile
import glob
import os

def main():
    print("Enter path of compressed data")
    path = input()
    print("Enter path to extract data")
    path_extract = input()

    zip_filepaths = glob.glob(os.path.join(path_extract, "*.ZIP"))

    for file in zip_filepaths:
        # File path with .ZIP removed
        zip_dir_name = os.path.splitext(file)[0]
        # Returns name of zip file
        unzip_dir_name = "\\".join(zip_dir_name.strip("\\").split("\\")[1:])
        # unzip_dir_name =  os.path.split(os.path.splitext(zip_dir_name)[0])[1]
        extract_path = os.path.join(path, unzip_dir_name)

        print(os.path.split(file)[1] + ":", end=" ")
        
        try:
            with zipfile.ZipFile(file, "r") as zip_ref:
                zip_ref.extractall(path=extract_path)
        except:
            print("failed")
        else:
            print("extracted")


if __name__ == "__main__":
    main()