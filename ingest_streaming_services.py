import csv
import logging
import requests
import dask.dataframe as dd

logging.basicConfig(filename="logs.log", level=logging.INFO)
# truncating log file before new run
with open("logs.log", "w"):
    pass

'''
use later to convert the CSVs from all the streaming services into a single
parquet file that gets loaded to GCS

df = dd.read_csv('./data/people/*.csv')
df = df.repartition(npartitions=1)
df.to_parquet('./tmp/people_parquet4')
'''
