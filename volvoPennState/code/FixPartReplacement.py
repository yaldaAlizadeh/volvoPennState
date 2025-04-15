#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().run_line_magic('env', 'JAVA_HOME=/usr/lib/jvm/java-11-openjdk-11.0.25.0.9-2.el8.x86_64')
get_ipython().run_line_magic('env', 'PATH=/storage/home/yqf5148/work/anaconda3/envs/volvopennstate-env/bin:storage/icds/swst/deployed/production/20220813/apps/anaconda3/2021.05_gcc-8.5.0/bin:/usr/lib/jvm/java-11-openjdk-11.0.25.0.9-2.el8.x86_64/bin/java:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/storage/icds/tools/bin:/storage/sys/slurm/bin:/storage/icds/tools/bin:/storage/sys/slurm/bin:/storage/icds/tools/bin:/storage/sys/slurm/bin:/storage/icds/tools/bin:/storage/sys/slurm/bin:/storage/icds/tools/bin:/storage/sys/slurm/bin')


# In[2]:


import findspark
import pandas as pd
import numpy as np

import pyspark as psk
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.utils import AnalysisException

import time as t 
from datetime import date, datetime, timedelta
from joblib import Parallel, delayed
import multiprocessing
from functools import reduce  
from math import modf
import sqlite3
import os
import sys

from delta import * 
from delta.tables import *
from delta import configure_spark_with_delta_pip
from IPython import get_ipython
import csv

findspark.init()
findspark.find()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

conf = (SparkConf().set("spark.driver.maxResultSize", "4g"))

# Create new context
sc = SparkContext(conf=conf)


# sc = SparkContext("local", "Simple App")

# Create SparkSession 
spark = SparkSession.builder        .master("local[2]")        .appName("test")        .config("spark.driver.maxResultSize", "20g")       .config("spark.driver.memory", "100g")       .getOrCreate()

#both works
# 1: 
# spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")    #To resolve the error for p1075_38 to_timestamp formating: You may get a different result due to the upgrading to Spark >= 3.0: Fail to parse '1/2/2019 20:40:00' in the new parser. You can set spark.sql.legacy.timeParserPolicy to LEGACY to restore the behavior before Spark 3.0, or set to CORRECTED and treat it as an invalid datetime string.
# Set Spark SQL legacy time parser policy to LEGACY to handle older date formats
# 2:
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# Increase the max fields in the string representation of a plan
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)  # Increase to 1000 or more as needed


# In[ ]:


spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/data/dataset/VINs_data_part_repl.csv")           .createOrReplaceTempView("VINs_data1")

# spark.read.option("header",True) \
#           .csv("/storage/home/yqf5148/work/volvoPennState/PopulationWithChassisId.csv") \
#           .createOrReplaceTempView("population")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/CCA Claims.csv")           .createOrReplaceTempView("cca_claims")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/EGR Cooler Claims.csv")           .createOrReplaceTempView("egr_cooler_claims")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/EGR FG 293 Claims.csv")           .createOrReplaceTempView("egr_fg_293_claims")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/EGR Sensors.csv")           .createOrReplaceTempView("egr_sensors")


# In[ ]:


# def if_part_is_replaced_for_this_VIN_in_this_timespan(thisVIN, start_date, end_date):
#     fault_date_time_format = 'M/d/yyyy'
    
#     # Loading all claims tables for the specific VIN
#     df_cca_claims = spark.sql(f"SELECT * FROM cca_claims WHERE VIN = '{thisVIN}'")
#     df_egr_cooler_claims = spark.sql(f"SELECT * FROM egr_cooler_claims WHERE VIN = '{thisVIN}'")
#     df_egr_fg_293_claims = spark.sql(f"SELECT * FROM egr_fg_293_claims WHERE VIN = '{thisVIN}'")
#     df_egr_sensors_claims = spark.sql(f"SELECT * FROM egr_sensors WHERE VIN = '{thisVIN}'")
    
#     # Define filtering condition
#     filter_condition = (
#         (f.to_timestamp(f.col('CLAIM_REG_DATE'), fault_date_time_format) > f.to_timestamp(f.lit(start_date), fault_date_time_format)) &
#         (f.to_timestamp(f.col('CLAIM_REG_DATE'), fault_date_time_format) < f.to_timestamp(f.lit(end_date), fault_date_time_format)) &
#         (f.col('TOT_CLAIM_PAYMENT_USD') > 1000.0)
#     )
    
#     # Filter dataframes based on the condition
#     df_cca_claims_part_replacements = df_cca_claims.filter(filter_condition)
#     df_egr_cooler_claims_part_replacements = df_egr_cooler_claims.filter(filter_condition)
#     df_egr_fg_293_claims_part_replacements = df_egr_fg_293_claims.filter(filter_condition)
#     df_egr_sensors_claims_part_replacements = df_egr_sensors_claims.filter(filter_condition)
    
#     # Count replacements in each table
#     total_replacements = (
#         df_cca_claims_part_replacements.count() +
#         df_egr_cooler_claims_part_replacements.count() +
#         df_egr_fg_293_claims_part_replacements.count() +
#         df_egr_sensors_claims_part_replacements.count()
#     )
    
#     return 1 if total_replacements > 0 else 0


# In[ ]:


def if_part_is_replaced_for_this_VIN_in_this_timespan(thisVIN, start_date, end_date):

    # Initialize an empty list to store the results
    replacement_records = []

    fault_date_time_format = 'MM/dd/yyyy'
    start_date = '2014-12-31'
    end_date = '2021-12-31'

    # Load all claims tables for the specific VIN
    claims_datasets = {
        'cca_claims': f"SELECT VIN, CLAIM_REG_DATE, TOT_CLAIM_PAYMENT_USD FROM cca_claims WHERE VIN = '{thisVIN}'",
        'egr_cooler_claims': f"SELECT VIN, CLAIM_REG_DATE, TOT_CLAIM_PAYMENT_USD FROM egr_cooler_claims WHERE VIN = '{thisVIN}'",
        'egr_fg_293_claims': f"SELECT VIN, CLAIM_REG_DATE, TOT_CLAIM_PAYMENT_USD FROM egr_fg_293_claims WHERE VIN = '{thisVIN}'",
        'egr_sensors_claims': f"SELECT VIN, CLAIM_REG_DATE, TOT_CLAIM_PAYMENT_USD FROM egr_sensors WHERE VIN = '{thisVIN}'"
    }

    # Define filtering condition with corrected date format
    for dataset_name, query in claims_datasets.items():
        try:
            df_claims = spark.sql(query)

            if df_claims is not None and df_claims.count() > 0:

                # df_claims.printSchema()  # Debugging step
                # df_claims.show(5, truncate=False)  # Show sample data

                df_claims = df_claims.withColumn("CLAIM_REG_DATE", f.to_date(f.col("CLAIM_REG_DATE"), "MM/dd/yyyy"))

                # Try alternative filtering
                df_filtered = df_claims.filter(
                    (f.col('CLAIM_REG_DATE') >= f.to_date(f.lit(start_date), "yyyy-MM-dd")) &
                    (f.col('CLAIM_REG_DATE') <= f.to_date(f.lit(end_date), "yyyy-MM-dd")) &
                    (f.col('TOT_CLAIM_PAYMENT_USD') > 1000.0)
                )

                # df_filtered.show(5, truncate=False)  # Show filtered data

                if df_filtered is not None and df_filtered.count() > 0:
                    for row in df_filtered.collect():
                        replacement_records.append([thisVIN, dataset_name, row['CLAIM_REG_DATE'], row['TOT_CLAIM_PAYMENT_USD']])

        except AnalysisException as e:
            print(f"Error processing dataset {dataset_name} for VIN {thisVIN}: {e}")


        if replacement_records:
            return 1
        else:
            return 0


# In[8]:


# Loop through the arguments and print them
if len(sys.argv) > 1:
    thisVIN = sys.argv[1]
    
    comma_delimited_string_for_all_columns_names = sys.argv[2]
    all_columns_names = comma_delimited_string_for_all_columns_names.split(',')
        
    the_calculator_jobID_for_thisVIN = sys.argv[3]
    
    print(f"the_calculator_jobID_for_thisVIN: {the_calculator_jobID_for_thisVIN} \n")
    file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs2/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")

    file.close()


# In[ ]:


# File paths
input_file_path = '/storage/home/yqf5148/work/volvoPennState/data/dataset/resultedData.csv'
cleaned_file_path = '/storage/home/yqf5148/work/volvoPennState/data/dataset/cleaned_resultedData.csv'

# Load and clean the CSV
cleaned_resultedData = pd.read_csv(cleaned_file_path, header=None, names=all_columns_names, index_col=False, dtype='unicode')
print(f"cleaned_resultedData is read.")
file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs2/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
file.writelines([f"cleaned_resultedData is read."])
file.writelines(cleaned_resultedData)
file.close()


# In[ ]:


from datetime import datetime, timedelta
import os
import pandas as pd

# Base date for calculating the 15-day intervals
base_date = datetime.strptime('12/31/2021', '%m/%d/%Y')
min_date = datetime.strptime('12/31/2014', '%m/%d/%Y')  # Restrict processing to dates after 01/01/2016
max_calendar_day = (base_date - min_date).days

# Initialize variables for batch processing
batch_size = 50
rows_to_write = []

# Check if cleaned file already exists, to determine whether to write headers
write_header = not os.path.exists(cleaned_file_path)

# Filter the DataFrame to only process rows for this specific VIN
df = cleaned_resultedData[cleaned_resultedData['VIN'] == thisVIN]

# Track if any modification was made for this VIN
modification_made = 0

# Process each row
print(f"Starting to process data for VIN={thisVIN}...")
file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs2/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
file.writelines(f"Starting to process data for VIN={thisVIN}...\n")
file.close()

for idx, row in df.iterrows():
    VIN = row.iloc[1]  # Access the second column in the row
    
    # Convert calendar_day to integer, skipping non-numeric rows
    try:
        calendar_day = int(pd.to_numeric(row['calendar_day'], errors='coerce'))
    except ValueError:
        print(f"Skipping row {idx} due to invalid calendar_day value: {row['calendar_day']}")
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs2/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
        file.writelines(f"Skipping row {idx} due to invalid calendar_day value: {row['calendar_day']}\n")
        file.close()
        continue

    # Skip rows with calendar_day beyond the allowed range
    if calendar_day > max_calendar_day:
        print(f"Skipping row {idx} because calendar_day exceeds 7 years: {calendar_day}")
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs2/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
        file.writelines(f"Skipping row {idx} because calendar_day exceeds 7 years: {calendar_day}\n")
        file.close()
        continue

    specific_date = base_date - timedelta(days=calendar_day)
    print(f"Processing row {idx}, VIN: {VIN}, calendar_day: {calendar_day}")
    file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs2/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
    file.writelines(f"Processing row {idx}, VIN: {VIN}, calendar_day: {calendar_day}\n")
    file.close()

    # Initialize the replacement columns to zero
    row_updated = False
    if row["if_parts_replaced_in_1th_15d"] != 1:
        df.at[idx, "if_parts_replaced_in_1th_15d"] = 0
    if row["if_parts_replaced_in_2nd_15d"] != 1:
        df.at[idx, "if_parts_replaced_in_2nd_15d"] = 0

    # First 15-day interval (0 to 15 days back from specific_date)
    for day_offset in range(15):
        end_date = specific_date - timedelta(days=day_offset)
        start_date = end_date - timedelta(days=15)

        if if_part_is_replaced_for_this_VIN_in_this_timespan(VIN, start_date, end_date) == 1:
            df.at[idx, "if_parts_replaced_in_1th_15d"] = 1
            row_updated = True
            modification_made = 1  # Set modification_made to 1 if any modification occurs
            print(f"  - Part replacement detected in the first 15-day interval for row {idx}.")
            file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs2/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
            file.writelines(f"  - Part replacement detected in the first 15-day interval for row {idx}.\n")
            file.close()
            break

    # Second 15-day interval (15 to 30 days back from specific_date)
    for day_offset in range(15, 30):
        end_date = specific_date - timedelta(days=day_offset)
        start_date = end_date - timedelta(days=15)

        if if_part_is_replaced_for_this_VIN_in_this_timespan(VIN, start_date, end_date) == 1:
            df.at[idx, "if_parts_replaced_in_2nd_15d"] = 1
            row_updated = True
            modification_made = 1  # Set modification_made to 1 if any modification occurs
            print(f"  - Part replacement detected in the second 15-day interval for row {idx}.")
            file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs2/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
            file.writelines(f"  - Part replacement detected in the second 15-day interval for row {idx}.\n")
            file.close()
            break

    # Add the modified row to the list if updated
    if row_updated:
        rows_to_write.append((idx, row))  # Append as a tuple of (index, row)
        print(f"  - Row {idx} marked for writing due to updates.")
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs2/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
        file.writelines(f"  - Row {idx} marked for writing due to updates.\n")
        file.close()

    # Write every 50 rows and reset buffer
    if (idx + 1) % batch_size == 0 or idx == len(df) - 1:
        if rows_to_write:
            print(f"Writing {len(rows_to_write)} rows to the output file at row {idx}.")
            file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs2/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
            file.writelines(f"Writing {len(rows_to_write)} rows to the output file at row {idx}.\n")
            file.close()

            rows_to_write_df = pd.DataFrame([row for idx, row in rows_to_write])
            rows_to_write_df.index = [idx for idx, row in rows_to_write]  # Set indices based on the original indices

            cleaned_resultedData.update(rows_to_write_df)
            cleaned_resultedData.to_csv(cleaned_file_path, index=False)

            write_header = False  # Only write the header for the first batch
            rows_to_write = []  # Reset list for the next batch

# If there are any remaining rows in rows_to_write, write them as well
if rows_to_write:
    print(f"Writing remaining {len(rows_to_write)} rows to the output file.")
    rows_to_write_df = pd.DataFrame([row for idx, row in rows_to_write])
    rows_to_write_df.index = [idx for idx, row in rows_to_write]

    cleaned_resultedData.update(rows_to_write_df)
    cleaned_resultedData.to_csv(cleaned_file_path, index=False)

# Mark this VIN as 'checked' in a separate file along with the modification status
VIN_feature_columns = pd.DataFrame({'VIN': [thisVIN], 'modification_made': [modification_made]})
VIN_feature_columns.to_csv('/storage/home/yqf5148/work/volvoPennState/data/dataset/VINs_data_part_repl.csv', index=False, mode='a', header=False) 
print(f"{thisVIN} is marked as 'checked' for the part replacement with modification status {modification_made}.")


# In[ ]:




