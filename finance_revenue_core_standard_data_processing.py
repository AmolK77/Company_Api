# Databricks notebook source
"""
As a temporary solution for the Finance - Revenue Core case of the accounting_use_case,
 we are downloading the finance_revenue_core reports from AWS and uploading it into Databricks
 until the phase 2 is finished and provides the data automatically.

The steps are:

1. Go to AWS -> Amazon S3 -> Buckets -> babbeldataeng-processed-staging -> finance/ -> finance_revenue_monthly/
(note that " babbeldataeng-processed-staging" is the productive bucket even though the name suggests otherwise)
(https://s3.console.aws.amazon.com/s3/buckets/babbeldataeng-processed-staging?region=eu-west-1&prefix=finance/finance_revenue_monthly/&showversions=false)

2. Sort the files by "last modified"

3. download the latest file and rename it on your system as follows:
    if it was generated before the 15th, add "_prelim" to the end of the file
    if it was generated at or after the 15th, add "_final" to the end of the file

4. Save the file in a folder "input" inside the Databricks Folder 
"/FileStore/finance_revenue_core/finance_revenue_core_standard" folder

5. run this python-script. it will add a column to the data according to the suffix of the file.

6. The resultfile will show up in Databricks - DBFS: /FileStore/finance_revenue_core/finance_revenue_core_standard/output


"""
#--------------------------------------------------------------------------------------------------#
# Initialization and parsing arguments


from io import StringIO
import email
import re
import boto3
import pandas as pd
import os

import glob
from pathlib import Path


# Initialization and parsing arguments
#--------------------------------------------------------------------------------------------------#

input_directory = "/FileStore/finance_revenue_core/finance_revenue_core_standard/input/"
output_directory = "/FileStore/finance_revenue_core/finance_revenue_core_standard/output/"

def write_df_to_dbfs(df, data_target):
    sparkdf = spark.createDataFrame(df)
    sparkdf.coalesce(1).write.format("csv").mode("overwrite").save(data_target, header = 'true')
    files = dbutils.fs.ls(data_target)
    csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
    dbutils.fs.mv(csv_file, data_target.rstrip('/') + ".csv")
    dbutils.fs.rm(data_target, recurse = True)

l_fileinfo = dbutils.fs.ls(input_directory)

for file in l_fileinfo:
    print(file.name)
    #read file
    sparkdf = spark.read.format("csv") \
        .option("inferSchema", "false") \
        .option("header", "true") \
        .option("sep", ",") \
        .load(input_directory + file.name)
    
    #transform data using python
    #display(sparkdf)
    df = sparkdf.toPandas()
    if file.name[-9:-4] == 'final':
        df['prelim_final']= 'final'
    elif file.name[-10:-4] == 'prelim':
        df['prelim_final']= 'prelim'

    #write file to dbfs as csv
    write_df_to_dbfs(df, output_directory + file.name )
    #df.to_csv(base_directory + '\\' + file, header=True, index=False )




# for file in directory.glob():
#     print(file)
