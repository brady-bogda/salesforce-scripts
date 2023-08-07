import csv
import pandas as pd

path = "data\\2019\\Salesforce Export 1.6.19\\Account.csv"
# data = open(path, "r")
# print(data)

# df = pd.read_csv(path, encoding="cp1252")
df = pd.read_csv(path, encoding="cp1252", engine="python")
# df = pd.read_csv(path, encoding="cp1252", low_memory=False)
