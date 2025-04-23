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


from filelock import FileLock


findspark.init()
findspark.find()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

conf = (SparkConf().set("spark.driver.maxResultSize", "12g"))

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


def raw_data_cleaning(dtc_type):
    df = spark.sql("SELECT * FROM {} WHERE _C0 IS NOT NULL AND _C0 != 'NA' AND _C0 != '' AND MESSAGE_ID IS NOT NULL AND MESSAGE_ID != '' AND MESSAGE_ID != 'NA' AND FAULT_DATE_TIME IS NOT NULL AND FAULT_DATE_TIME != '' AND FAULT_DATE_TIME != 'NA' AND FAULT_STATUS IS NOT NULL AND FAULT_STATUS != '' AND FAULT_STATUS != 'NA' AND VIN IS NOT NULL AND VIN != '' AND VIN != 'NA' AND length(VIN) !< 17".format(dtc_type)) 
    return df


# In[4]:


# Read CSV file into table

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/data/dataset/VINs_data.csv")           .createOrReplaceTempView("VINs_data")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/PopulationWithChassisId.csv")           .createOrReplaceTempView("population")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/CCA Claims.csv")           .createOrReplaceTempView("cca_claims")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/EGR Cooler Claims.csv")           .createOrReplaceTempView("egr_cooler_claims")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/EGR FG 293 Claims.csv")           .createOrReplaceTempView("egr_fg_293_claims")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/EGR Sensors.csv")           .createOrReplaceTempView("egr_sensors")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/Engine_Emissions_Table1.csv")           .createOrReplaceTempView("engine_emissions_table1")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/Engine_Emissions_Table2.csv")           .createOrReplaceTempView("engine_emissions_table2")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/P1075_38.csv")           .createOrReplaceTempView("p1075_38")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/P1075_75.csv")           .createOrReplaceTempView("p1075_75")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/P1075_77.csv")           .createOrReplaceTempView("p1075_77")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/P1075_86.csv")           .createOrReplaceTempView("p1075_86")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/P1075_92.csv")           .createOrReplaceTempView("p1075_92")
 
spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/P1075_94.csv")           .createOrReplaceTempView("p1075_94")   

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/P0401_faults.csv")           .createOrReplaceTempView("p0401_faults")

spark.read.option("header",True,)           .csv("/storage/home/yqf5148/work/volvoPennState/P2457_faults.csv")           .createOrReplaceTempView("p2457_faults")


# In[5]:


df_population = spark.sql("SELECT * FROM population")

df_p1075_38 = raw_data_cleaning('p1075_38')

df_p1075_75 = raw_data_cleaning('p1075_75')

df_p1075_77 = raw_data_cleaning('p1075_77')

df_p1075_86 = raw_data_cleaning('p1075_86')

df_p1075_92 = raw_data_cleaning('p1075_92')

df_p1075_94 = raw_data_cleaning('p1075_94')

df_p0401 = raw_data_cleaning('p0401_faults')

df_p2457 = raw_data_cleaning('p2457_faults')


# In[9]:


def fix_problem_of_fault_date_time_with_no_seconds(df, dtc_type):
    if dtc_type=="p1075_38":
       return f.to_timestamp(f.concat(df.FAULT_DATE_TIME, lit(":00")), "M/d/yyyy HH:mm:ss")
    else:
       return f.to_timestamp(df.FAULT_DATE_TIME, "yyyy-MM-dd HH:mm:ss")


def feature_1_or_2_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN, dtc_status_to_calculate_this_feature):
      
    df_dtc_type_and_status_for_this_VIN = df_dtc_type_for_this_VIN.filter(f.col('FAULT_STATUS') == dtc_status_to_calculate_this_feature)

    return df_dtc_type_and_status_for_this_VIN.count()


def feature_3_or_4_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN, dtc_type, dtc_status_to_calculate_this_feature):

    fault_date_time_format='yyyy-MM-dd HH:mm:ss'
    if dtc_type=="p1075_38":
        fault_date_time_format='M/d/yyyy HH:mm'

    dtcs_with_all_statuses_df = df_dtc_type_for_this_VIN.sort(f.to_timestamp(f.col("FAULT_DATE_TIME"), fault_date_time_format))


    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df

#     PsSpark: This one is also true to access the first row of pyspark dataframe    
#     headRowId = dtcs_with_all_statuses_df2.first()[0]

    tailRowId = dtcs_with_all_statuses_df.tail(1)[0][0]
    dtcs_with_all_statuses_df = dtcs_with_all_statuses_df.filter(f.col('_c0') != tailRowId).fillna(0)
    dtcs_with_all_statuses_df = dtcs_with_all_statuses_df.sort(f.to_timestamp(f.col("FAULT_DATE_TIME"), fault_date_time_format)).withColumn("_c0",monotonically_increasing_id())
    dtcs_with_all_statuses_df = dtcs_with_all_statuses_df.dropna()
    dtcs_with_all_statuses_df = dtcs_with_all_statuses_df.withColumnRenamed("FAULT_STATUS", "FAULT_STATUS_CURRENT_DTC")
    dtcs_with_all_statuses_df = dtcs_with_all_statuses_df.withColumnRenamed("FAULT_DATE_TIME", "FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC")

    headRowId = dtcs_with_all_statuses_df2.head(1)[0][0]
    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df2.filter(f.col('_c0') != headRowId).fillna(0)
    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df2.sort(f.to_timestamp(f.col("FAULT_DATE_TIME"), fault_date_time_format)).withColumn("_c0",monotonically_increasing_id())
    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df2.dropna()
    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df2.withColumnRenamed("FAULT_STATUS", "FAULT_ANY_STATUS_NEXT_DTC")
    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df2.withColumnRenamed("FAULT_DATE_TIME", "FAULT_DATE_TIME_NEXT_ANY_DTC")
    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df2.select([c for c in dtcs_with_all_statuses_df2.columns if c != "VIN"])

    # create list of dataframes
    list_df = [dtcs_with_all_statuses_df, dtcs_with_all_statuses_df2]

    # merge all at once
    my_temp_df = reduce(lambda x, y: x.join(y, on="_c0"), list_df)
    my_temp_df = my_temp_df.withColumn("FAULT_DATE_TIME_DIFF", (f.unix_timestamp(f.col("FAULT_DATE_TIME_NEXT_ANY_DTC"), format=fault_date_time_format) - f.unix_timestamp(f.col("FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC"), format=fault_date_time_format)))     .withColumn("DURATION_IN_THIS_15_DAYS_BACK", f.when((f.to_timestamp(f.col("FAULT_DATE_TIME_NEXT_ANY_DTC"), fault_date_time_format) <= end_date) & 
                                                                  (f.to_timestamp(f.col("FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC"), fault_date_time_format) >= start_date), 
                                                                  (f.unix_timestamp("FAULT_DATE_TIME_NEXT_ANY_DTC", format=fault_date_time_format) - f.unix_timestamp("FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC", format=fault_date_time_format)))) \
    .withColumn("COUNT_IN_THIS_15_DAYS_BACK", f.when((f.to_timestamp(f.col("FAULT_DATE_TIME_NEXT_ANY_DTC"), fault_date_time_format) <= end_date) & 
                                                                  (f.to_timestamp(f.col("FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC"), fault_date_time_format) >= start_date), (f.lit(1)).cast(IntegerType())))
    start_only_with_this_status_dtcs_df = my_temp_df.filter(f.col("FAULT_STATUS_CURRENT_DTC") == f.lit(dtc_status_to_calculate_this_feature))


    duration_of_specific_type_dtcs_for_this_VIN = start_only_with_this_status_dtcs_df.select(f.sum("DURATION_IN_THIS_15_DAYS_BACK")).fillna(0)
    count_of_specific_type_dtcs_for_this_VIN = start_only_with_this_status_dtcs_df.select(f.sum("COUNT_IN_THIS_15_DAYS_BACK")).fillna(1)
    duration = duration_of_specific_type_dtcs_for_this_VIN.collect()[0][0]
    return duration


  

def feature_5_or_6_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN, dtc_type, dtc_status_to_calculate_this_feature):
    
    fault_date_time_format='yyyy-MM-dd HH:mm:ss'
    if dtc_type=="p1075_38":
        fault_date_time_format='M/d/yyyy HH:mm'


    dtcs_with_all_statuses_df = df_dtc_type_for_this_VIN.sort(f.to_timestamp(f.col("FAULT_DATE_TIME"), fault_date_time_format))


    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df

#     PsSpark: This one is also true to access the first row of pyspark dataframe    
    tailRowId = dtcs_with_all_statuses_df.tail(1)[0][0]
    dtcs_with_all_statuses_df = dtcs_with_all_statuses_df.filter(f.col('_c0') != tailRowId).fillna(0)
    dtcs_with_all_statuses_df = dtcs_with_all_statuses_df.sort(f.to_timestamp(f.col("FAULT_DATE_TIME"), fault_date_time_format)).withColumn("_c0",monotonically_increasing_id())
    dtcs_with_all_statuses_df = dtcs_with_all_statuses_df.dropna()
    dtcs_with_all_statuses_df = dtcs_with_all_statuses_df.withColumnRenamed("FAULT_STATUS", "FAULT_STATUS_CURRENT_DTC")
    dtcs_with_all_statuses_df = dtcs_with_all_statuses_df.withColumnRenamed("FAULT_DATE_TIME", "FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC")

    headRowId = dtcs_with_all_statuses_df2.head(1)[0][0]
    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df2.filter(f.col('_c0') != headRowId).fillna(0)
    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df2.sort(f.to_timestamp(f.col("FAULT_DATE_TIME"), fault_date_time_format)).withColumn("_c0",monotonically_increasing_id())
    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df2.dropna()
    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df2.withColumnRenamed("FAULT_STATUS", "FAULT_ANY_STATUS_NEXT_DTC")
    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df2.withColumnRenamed("FAULT_DATE_TIME", "FAULT_DATE_TIME_NEXT_ANY_DTC")
    dtcs_with_all_statuses_df2 = dtcs_with_all_statuses_df2.select([c for c in dtcs_with_all_statuses_df2.columns if c != "VIN"])


    # create list of dataframes
    list_df = [dtcs_with_all_statuses_df, dtcs_with_all_statuses_df2]

    # merge all at once
    my_temp_df = reduce(lambda x, y: x.join(y, on="_c0"), list_df)
    my_temp_df = my_temp_df.withColumn("FAULT_DATE_TIME_DIFF", (f.unix_timestamp(f.col("FAULT_DATE_TIME_NEXT_ANY_DTC"), format=fault_date_time_format) - f.unix_timestamp(f.col("FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC"), format=fault_date_time_format)))     .withColumn("DURATION_IN_THIS_15_DAYS_BACK", f.when((f.to_timestamp(f.col("FAULT_DATE_TIME_NEXT_ANY_DTC"), fault_date_time_format) <= end_date) & 
                                                                  (f.to_timestamp(f.col("FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC"), fault_date_time_format) >= start_date), 
                                                                  (f.unix_timestamp("FAULT_DATE_TIME_NEXT_ANY_DTC", format=fault_date_time_format) - f.unix_timestamp("FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC", format=fault_date_time_format)))) \
    .withColumn("COUNT_IN_THIS_15_DAYS_BACK", f.when((f.to_timestamp(f.col("FAULT_DATE_TIME_NEXT_ANY_DTC"), fault_date_time_format) <= end_date) & 
                                                                  (f.to_timestamp(f.col("FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC"), fault_date_time_format) >= start_date), (f.lit(1)).cast(IntegerType())))
#     my_temp_df["_c0", "VIN","FAULT_STATUS_CURRENT_DTC", "FAULT_ANY_STATUS_NEXT_DTC", "FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC", "FAULT_DATE_TIME_NEXT_ANY_DTC", f.to_timestamp(f.col("FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC"), fault_date_time_format), f.to_timestamp(f.col("FAULT_DATE_TIME_NEXT_ANY_DTC"), fault_date_time_format)].show(194)
    start_only_with_this_status_dtcs_df = my_temp_df.filter(f.col("FAULT_STATUS_CURRENT_DTC") == f.lit(dtc_status_to_calculate_this_feature))



    duration_of_specific_type_dtcs_for_this_VIN = start_only_with_this_status_dtcs_df.select(f.sum("DURATION_IN_THIS_15_DAYS_BACK")).fillna(0)
    count_of_specific_type_dtcs_for_this_VIN = start_only_with_this_status_dtcs_df.select(f.sum("COUNT_IN_THIS_15_DAYS_BACK")).fillna(1)
    duration = duration_of_specific_type_dtcs_for_this_VIN.collect()[0][0]

    total_count = count_of_specific_type_dtcs_for_this_VIN.collect()[0][0]
    if(total_count != 0): 
        average_of_specific_type_dtcs_for_this_VIN = duration/total_count
        return average_of_specific_type_dtcs_for_this_VIN
    else:
        return 0.0
  
  
def feature_7_or_8_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN, vehicle_speed, dtc_status_to_calculate_this_feature):
    
    df_dtc_type_and_status_for_this_VIN = df_dtc_type_for_this_VIN                .filter(df_dtc_type_for_this_VIN.FAULT_STATUS == dtc_status_to_calculate_this_feature)                .filter(df_dtc_type_for_this_VIN.ROAD_SPEED_MPH == vehicle_speed)
             
    return df_dtc_type_and_status_for_this_VIN.count()



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



  
  
# Normalize using Min/Max Normalization.
def normalize_numeric_feature_values(statement_in_feature_name, df):
    selected_col_names_list = [col for col in df.columns.values if statement_in_feature_name in col]   # selects names of columns that contain specific string
    selected_cols = df[selected_col_names_list]
    
    for col_name in selected_col_names_list:
        selected_cols[col_name] = selected_cols[col_name].str.replace(",",".")
        selected_cols[col_name] = selected_cols[col_name].apply(lambda x: float(x.split()[0]))
        
    selected_cols_norm = selected_cols.apply(lambda iterator: ((iterator - iterator.mean())/(iterator.max() - iterator.min())).round(3))
    
    df[selected_cols_norm.columns] = selected_cols_norm
    return df

            
def move_over_calendar_and_compute_features(df_selected_features_from_population_for_this_VIN, thisVIN, new_15day_end_date, span_length, dayCount, jobID):
    if dayCount > 2557:
            print(f"Skipping calendar_day={dayCount} as it exceeds 2557.")
            return
    
    file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{jobID}.txt", "a")
    file.writelines(f"A new day move on calendar: thisVIN={thisVIN}, new_15day_end_date={new_15day_end_date}, span_length={span_length}, dayCount={dayCount} \n")
    file.close()
    print(f"A new day move on calendar: thisVIN={thisVIN}, new_15day_end_date={new_15day_end_date}, span_length={span_length}, dayCount={dayCount} \n") 

#     schema = StructType([])   ***** This does not work. Creating EmptyRDD does not allow to add further columns later using withColumn  ****
#     df_features_for_this_VIN_and_this_dayCount = sqlContext.createDataFrame(sc.emptyRDD(), schema)
#     df_features_for_this_VIN_and_this_dayCount = df_features_for_this_VIN_and_this_dayCount.withColumn("VIN", f.lit(thisVIN)).withColumn("calendar_day", (f.lit(dayCount)).cast(IntegerType()))

    
    schema = StructType([StructField('VIN', StringType(), True),
                      StructField('calendar_day', IntegerType(), True)])
    data = [
        (thisVIN, dayCount)
      ]
    df_calculated_features_for_this_VIN_and_this_dayCount = spark.createDataFrame(data = data, schema = schema)

    new_15day_start_date = new_15day_end_date - timedelta(days = dayCount)

    previous_15day_duration_end_date = new_15day_start_date
    previous_15day_duration_start_date = previous_15day_duration_end_date - timedelta(days = dayCount)



    list_of_dtc_type = ['p1075_38', 'p1075_75', 'p1075_77', 'p1075_86', 'p1075_92', 'p1075_94', 'p0401_faults', 'p2457_faults']
    list_of_dtc_df = [df_p1075_38, df_p1075_75, df_p1075_77, df_p1075_86, df_p1075_92, df_p1075_94, df_p0401, df_p2457]
    
    for i in range(len(list_of_dtc_df)):
        dtc_type_df = list_of_dtc_df[i]
        
        
        #filter dtc-type database for thisVIN and then check if there is any dtc's related to this VIN in the first 15 days timespan before "new_15day_end_date" and another 15 days before this period.
        df_dtc_type_for_this_VIN = dtc_type_df.filter(f.col('VIN')==thisVIN)
        
        df_dtc_type_for_this_VIN_in_first_15day_timespan = df_dtc_type_for_this_VIN.filter((fix_problem_of_fault_date_time_with_no_seconds(df_dtc_type_for_this_VIN, list_of_dtc_type[i]) > new_15day_start_date) & (fix_problem_of_fault_date_time_with_no_seconds(df_dtc_type_for_this_VIN, list_of_dtc_type[i]) < new_15day_end_date))
        df_dtc_type_for_this_VIN_in_previous_15day_timespan = df_dtc_type_for_this_VIN.filter((fix_problem_of_fault_date_time_with_no_seconds(df_dtc_type_for_this_VIN, list_of_dtc_type[i]) > previous_15day_duration_start_date) & (fix_problem_of_fault_date_time_with_no_seconds(df_dtc_type_for_this_VIN, list_of_dtc_type[i]) < previous_15day_duration_end_date))
        
        count1 = df_dtc_type_for_this_VIN_in_first_15day_timespan.count()
        count2 = df_dtc_type_for_this_VIN_in_previous_15day_timespan.count()
        '''
        Feature 1
        
        Definition: number of alerts over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{jobID}.txt", "a")
        file.writelines(f"feature 1 for {thisVIN} in day {dayCount} and dtc_type {list_of_dtc_type[i]} \n")
        file.close()
        print(f"feature 1 for {thisVIN} in day {dayCount} and dtc_type {list_of_dtc_type[i]} \n")
        
        if count1 > 0:
            feature_1_value_first_15days = feature_1_or_2_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_first_15day_timespan, "Y")
        else:
            feature_1_value_first_15days = 0

        
        if count2 > 0:
            feature_1_value_second_15days = feature_1_or_2_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_previous_15day_timespan, "Y")
        else:
            feature_1_value_second_15days = 0
            
            
            

        '''
        Feature 2
                
        Definition: number of intermittent alerts over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{jobID}.txt", "a")
        file.writelines(f"feature 2 for {thisVIN} in day {dayCount} \n")
        file.close()
        print(f"feature 2 for {thisVIN} in day {dayCount} \n")

        if count1 > 0:
            feature_2_value_first_15days = feature_1_or_2_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_first_15day_timespan, "I")
        else:
            feature_2_value_first_15days = 0
            
        if count2 > 0:           
            feature_2_value_second_15days = feature_1_or_2_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_previous_15day_timespan, "I")
        else:
            feature_2_value_second_15days = 0
            
            

        '''
        Feature 3
        
        Definition: duration of active alerts over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{jobID}.txt", "a")
        file.writelines(f"feature 3 for {thisVIN} in day {dayCount} \n")
        file.close()
        print(f"feature 3 for {thisVIN} in day {dayCount} \n")
        
        if count1 > 0:
            feature_3_value_first_15days = feature_3_or_4_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_first_15day_timespan, list_of_dtc_type[i], "Y")
        else:
            feature_3_value_first_15days = 0.0
            
        if count2 > 0:    
            feature_3_value_second_15days = feature_3_or_4_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_previous_15day_timespan, list_of_dtc_type[i], "Y")
        else:
            feature_3_value_second_15days = 0.0
            
            
        '''
        Feature 4:
                
        Definition: duration of intermittent alerts over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{jobID}.txt", "a")
        file.writelines(f"feature 4 for {thisVIN} in day {dayCount} \n")
        file.close()
        print(f"feature 4 for {thisVIN} in day {dayCount} \n")

        if count1 > 0:
            feature_4_value_first_15days = feature_3_or_4_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_first_15day_timespan, list_of_dtc_type[i], "I")
        else:
            feature_4_value_first_15days = 0.0
            
        if count2 > 0: 
            feature_4_value_second_15days = feature_3_or_4_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_previous_15day_timespan, list_of_dtc_type[i], "I")
        else:
            feature_4_value_second_15days = 0.0
            
        '''    
        Feature 5
                
        Definition: average time between active alerts over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{jobID}.txt", "a")
        file.writelines(f"feature 5 for {thisVIN} in day {dayCount} \n")
        file.close()
        print(f"feature 5 for {thisVIN} in day {dayCount} \n")

        if count1 > 0:
            feature_5_value_first_15days =  feature_5_or_6_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_first_15day_timespan, list_of_dtc_type[i], "Y")
        else:
            feature_5_value_first_15days =  0.0 
            
        if count2 > 0:    
            feature_5_value_second_15days = feature_5_or_6_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_previous_15day_timespan, list_of_dtc_type[i], "Y")
        else:
            feature_5_value_second_15days = 0.0
            
            
        '''
        Feature 6
                
        Definition: average time between intermittent alerts over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{jobID}.txt", "a")
        file.writelines(f"feature 6 for {thisVIN} in day {dayCount} \n")
        file.close()
        print(f"feature 6 for {thisVIN} in day {dayCount} \n")

        if count1 > 0:
            feature_6_value_first_15days =  feature_5_or_6_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_first_15day_timespan, list_of_dtc_type[i], "I")
        else:
            feature_6_value_first_15days =  0.0
            
        if count2 > 0:    
            feature_6_value_second_15days = feature_5_or_6_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_previous_15day_timespan, list_of_dtc_type[i], "I")
        else:
            feature_6_value_second_15days = 0.0
            
            
        '''
        Feature 7
                
        Definition: number of active alerts with speed = 0 over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{jobID}.txt", "a")
        file.writelines(f"feature 7 for {thisVIN} in day {dayCount} \n")
        file.close()
        print(f"feature 7 for {thisVIN} in day {dayCount} \n")

        if count1 > 0:        
            feature_7_value_first_15days =  feature_7_or_8_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_first_15day_timespan, 0, "Y")
        else:
            feature_7_value_first_15days =  0
        
        if count2 > 0:
            feature_7_value_second_15days = feature_7_or_8_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_previous_15day_timespan, 0, "Y")
        else:
            feature_7_value_second_15days = 0
            
            
        '''
        Feature 8
                
        Definition: number of intermittent alerts with speed = 0 over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{jobID}.txt", "a") 
        file.writelines(f"feature 8 for {thisVIN} in day {dayCount} \n")
        file.close()
        print(f"feature 8 for {thisVIN} in day {dayCount} \n")

        if count1 > 0:        
            feature_8_value_first_15days =  feature_7_or_8_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_first_15day_timespan, 0, "I")
        else:
            feature_8_value_first_15days =  0
            
        if count2 > 0:    
            feature_8_value_second_15days = feature_7_or_8_calculate_for_this_VIN_and_this_timespan(df_dtc_type_for_this_VIN_in_previous_15day_timespan, 0, "I")
        else:
            feature_8_value_second_15days = 0
            
            
  
        dtc = 'aaaaaa'

        if i == 0 :
          dtc = 'dtc38'
        elif i == 1:
          dtc = 'dtc75'
        elif i == 2:
          dtc = 'dtc77'
        elif i == 3:
          dtc = 'dtc86'
        elif i == 4:
          dtc = 'dtc92'
        elif i == 5:
          dtc = 'dtc94'
        elif i == 6:
          dtc = 'dtc0401'
        elif i == 7:
          dtc = 'dtc2457'

        #end of else for i values
        feature1Name1 = 'feature_1_'+dtc+'_first_15_days'
        feature1Name2 = 'feature_1_'+dtc+'_second_15_days'
        feature2Name1 = 'feature_2_'+dtc+'_first_15_days'
        feature2Name2 = 'feature_2_'+dtc+'_second_15_days'
        feature3Name1 = 'feature_3_'+dtc+'_first_15_days'
        feature3Name2 = 'feature_3_'+dtc+'_second_15_days'
        feature4Name1 = 'feature_4_'+dtc+'_first_15_days'
        feature4Name2 = 'feature_4_'+dtc+'_second_15_days'
        feature5Name1 = 'feature_5_'+dtc+'_first_15_days'
        feature5Name2 = 'feature_5_'+dtc+'_second_15_days'
        feature6Name1 = 'feature_6_'+dtc+'_first_15_days'
        feature6Name2 = 'feature_6_'+dtc+'_second_15_days'
        feature7Name1 = 'feature_7_'+dtc+'_first_15_days'
        feature7Name2 = 'feature_7_'+dtc+'_second_15_days'
        feature8Name1 = 'feature_8_'+dtc+'_first_15_days'
        feature8Name2 = 'feature_8_'+dtc+'_second_15_days'


        df_calculated_features_for_this_VIN_and_this_dayCount = df_calculated_features_for_this_VIN_and_this_dayCount                                                                                                .withColumn(feature1Name1, (f.lit(feature_1_value_first_15days)).cast(IntegerType()))                                                                                               .withColumn(feature1Name2, (f.lit(feature_1_value_second_15days)).cast(IntegerType()))                                                                                               .withColumn(feature2Name1, (f.lit(feature_2_value_first_15days)).cast(IntegerType()))                                                                                               .withColumn(feature2Name2, (f.lit(feature_2_value_second_15days)).cast(IntegerType()))                                                                                               .withColumn(feature3Name1, (f.lit(feature_3_value_first_15days)).cast(FloatType()))                                                                                               .withColumn(feature3Name2, (f.lit(feature_3_value_second_15days)).cast(FloatType()))                                                                                               .withColumn(feature4Name1, (f.lit(feature_4_value_first_15days)).cast(FloatType()))                                                                                               .withColumn(feature4Name2, (f.lit(feature_4_value_second_15days)).cast(FloatType()))                                                                                               .withColumn(feature5Name1, (f.lit(feature_5_value_first_15days)).cast(FloatType()))                                                                                               .withColumn(feature5Name2, (f.lit(feature_5_value_second_15days)).cast(FloatType()))                                                                                               .withColumn(feature6Name1, (f.lit(feature_6_value_first_15days)).cast(FloatType()))                                                                                               .withColumn(feature6Name2, (f.lit(feature_6_value_second_15days)).cast(FloatType()))                                                                                               .withColumn(feature7Name1, (f.lit(feature_7_value_first_15days)).cast(IntegerType()))                                                                                               .withColumn(feature7Name2, (f.lit(feature_7_value_second_15days)).cast(IntegerType()))                                                                                               .withColumn(feature8Name1, (f.lit(feature_8_value_first_15days)).cast(IntegerType()))                                                                                               .withColumn(feature8Name2, (f.lit(feature_8_value_second_15days)).cast(IntegerType()))
 
        #   for loop finished 
        
    df_calculated_features_for_this_VIN_and_this_dayCount = df_calculated_features_for_this_VIN_and_this_dayCount.withColumn("if_parts_replaced_in_first_15days", (f.lit(if_part_is_replaced_for_this_VIN_in_this_timespan(thisVIN, new_15day_start_date, new_15day_end_date))).cast(IntegerType()))  .withColumn("if_parts_replaced_in_second_15days", (f.lit(if_part_is_replaced_for_this_VIN_in_this_timespan(thisVIN, previous_15day_duration_start_date, previous_15day_duration_end_date))).cast(IntegerType()))
    print(f"we finally write something of length for {thisVIN} in day {dayCount} \n")

    file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{jobID}.txt", "a")
    file.writelines(f"we finally write something of length for {thisVIN} in day {dayCount} \n")
    file.close()
    
    vin_file_path = '/storage/home/yqf5148/work/volvoPennState/data/dataset/VINs_data.csv'
    lock_file_path = vin_file_path + '.lock'  # creates VINs_data.csv.lock
    lock = FileLock(lock_file_path)

    VINs_columns_names =['VIN','TOTAL_ROWS','CALCULATION']
    
    with lock:
        df_VINs = pd.read_csv(vin_file_path, sep=',', names=VINs_columns_names, header=None)
        print(f"🔓 Safely read from {vin_file_path}")
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{jobID}.txt", "a")
        file.writelines(f"🔓 Safely read from {vin_file_path}")
        file.close()
    
    df_VINs.loc[df_VINs['VIN'] == thisVIN, ['TOTAL_ROWS', 'CALCULATION']] = [dayCount, 'UP']

    
    with lock:  # This ensures only one job writes at a time
        df_VINs.to_csv(vin_file_path, index = None, mode = 'w', header=False)
        print(f"✔️ Wrote to {vin_file_path} safely with lock.")
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{jobID}.txt", "a")
        file.writelines(f"✔️ Wrote to {vin_file_path} safely with lock.")
        file.close()
    
    
    '''here we aggregate all the selected features from the population for this VIN with the 8 calculated feature values for this VIN 
    for this specific day and then write it to resultedData.csv as one data point.'''

    list_features_for_this_VIN_and_this_dayCount = [df_selected_features_from_population_for_this_VIN, ]
    df_features_for_this_VIN_and_this_dayCount = reduce(lambda x, y: x.join(y, on="VIN"), list_features_for_this_VIN_and_this_dayCount)
    df_features_for_this_VIN_and_this_dayCount.toPandas().to_csv('/storage/home/yqf5148/work/volvoPennState/data/dataset/resultedData.csv', index = None, mode = 'a', header=False) 
    return



def get_max_valid_calendar_day(ins_date_str, end_date_str="2021-12-31"):
    """
    Calculates the maximum valid calendar day (number of days back from end_date)
    for a VIN based on its installation date.
    """
    try:
        ins_date = datetime.strptime(ins_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        max_days = (end_date - ins_date).days
        return max_days if max_days >= 0 else 0
    except Exception as e:
        print(f"Error parsing INS_DATE {ins_date_str}: {e}")
        return 0
    
    
def get_ins_date_for_vin(vin, df_pop):
    row = df_pop[df_pop["VIN"] == vin]
    if not row.empty:
        return row.iloc[0]["INS_DATE"].strftime("%Y-%m-%d")  # Return as string
    else:
        print(f"❗ VIN {vin} not found in population data.")
        return None


# In[10]:


# Loop through the arguments and print them
if len(sys.argv) > 1:
    thisVIN = sys.argv[1]
    the_calculator_jobID_for_thisVIN = sys.argv[2]
    #erasing the txt file for output of the submitted job that runs this Notebook:
    # open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "w").close()
    
    
    print("Current VIN: {} \n".format(thisVIN))
    file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
    file.writelines(["Current VIN: {} \n".format(thisVIN)])
    file.close()
    
    ##### Features Generator Code:
    duration_end_date = '2021-12-31'

    day_delta = timedelta(days = 1)
    split_duration_end_date = duration_end_date.split('-')
    end_date = date(int(split_duration_end_date[0]), int(split_duration_end_date[1]), int(split_duration_end_date[2]))
    
    # Limit calendar_day range based on VIN's actual INS_DATE (installation date).
    # This avoids computing features for days before the engine started operating.

    # Load the population dataset
    df_population = spark.sql("SELECT * FROM population")
    # Filter to keep only VIN and INS_DATE columns
    df_population_filtered_spark = df_population[["VIN", "INS_DATE"]]
    df_population_filtered = df_population_filtered_spark.toPandas()
    
    # Make sure INS_DATE is parsed correctly
    df_population_filtered["INS_DATE"] = pd.to_datetime(df_population_filtered["INS_DATE"], errors="coerce")
    ins_date_str = get_ins_date_for_vin(thisVIN, df_population_filtered)
    
    max_dayCount = 2557
    if ins_date_str:
        max_dayCount = get_max_valid_calendar_day(ins_date_str)
    
    print(f"→ Max calendar_day for {thisVIN}: {max_dayCount}")
    file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
    file.writelines(f"→ Max calendar_day for {thisVIN}: {max_dayCount}")
    file.close()

    #minus 1 is to consider 12/31/2021 itself
    start_date = end_date - timedelta(days = max_dayCount - 1)

    print("start_date = {}, end_date = {}".format(start_date, end_date))
    file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
    file.writelines(["start_date = {}, end_date = {} \n".format(start_date, end_date)])
    file.close()

    span_length = 15


    VIN_feature_columns = StructType([StructField('VIN', StringType(), True),
                                      StructField('TOTAL_ROWS', IntegerType(), True),
                                      StructField('CALCULATION', StringType(), True)])
    df_new_VIN = spark.createDataFrame(data = [(thisVIN, 0, 'NF')], schema = VIN_feature_columns)
    # df_new_VIN.toPandas().to_csv('/storage/home/yqf5148/work/volvoPennState/data/dataset/VINs_data.csv', index = None, mode = 'a', header=False) 


    df_filtered_population_for_this_VIN = df_population_filtered.filter(f.col('VIN')==thisVIN)
    
    columns_of_population = ['VIN','ENGINE_SIZE','ENGINE_HP','VEH_TYPE']+[s for s in df_population.columns if 'KOLA' in s]
    df_selected_features_from_population_for_this_VIN = df_filtered_population_for_this_VIN[columns_of_population]
    
    if df_selected_features_from_population_for_this_VIN.count()!= 0 :
        
        how_many_month = int((end_date - start_date).days/15)
        print("how_many_month={} \n".format(how_many_month))
        file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
        file.writelines(["how_many_month={} \n".format(how_many_month)])
        file.close()
        if how_many_month == 0:
            remaining_days = int((end_date - start_date).days) 
            print("remaining_days={} \n".format(remaining_days))
            file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
            file.writelines(["remaining_days={} \n".format(remaining_days)])
            file.close()
            Parallel(n_jobs= 5, prefer="threads", batch_size=5)(delayed(move_over_calendar_and_compute_features)(df_selected_features_from_population_for_this_VIN, thisVIN, end_date, span_length, day_count_in_month, the_calculator_jobID_for_thisVIN) for day_count_in_month in range(0, remaining_days))
        else:
            for number_of_monthes_in_time_duration in range(0, how_many_month):
                print("number_of_monthes_in_time_duration={} \n".format(number_of_monthes_in_time_duration))
                file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
                file.writelines(["number_of_monthes_in_time_duration={} \n".format(number_of_monthes_in_time_duration)])
                file.close()
                if number_of_monthes_in_time_duration < how_many_month:
                    Parallel(n_jobs= 5, prefer="threads", batch_size=5)(delayed(move_over_calendar_and_compute_features)(df_selected_features_from_population_for_this_VIN, thisVIN, end_date, span_length, 15 * number_of_monthes_in_time_duration + day_count_in_month, the_calculator_jobID_for_thisVIN) for day_count_in_month in range(0, 15))   

                else:
                    remaining_days = int((end_date - start_date).days) - 15 * number_of_monthes_in_time_duration 
                    print("remaining_days={} \n".format(remaining_days))
                    file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
                    file.writelines(["remaining_days={} \n".format(remaining_days)])
                    file.close()
                    Parallel(n_jobs= 5, prefer="threads", batch_size=5)(delayed(move_over_calendar_and_compute_features)(df_selected_features_from_population_for_this_VIN, thisVIN, end_date, span_length, 15 * number_of_monthes_in_time_duration + day_count_in_month, the_calculator_jobID_for_thisVIN) for day_count_in_month in range(0, remaining_days))

        vin_file_path = '/storage/home/yqf5148/work/volvoPennState/data/dataset/VINs_data.csv'
        lock_file_path = vin_file_path + '.lock'  # creates VINs_data.csv.lock
        lock = FileLock(lock_file_path)

        VINs_columns_names =['VIN','TOTAL_ROWS','CALCULATION']
    
        with lock:
            df_VINs = pd.read_csv(vin_file_path, sep=',', names=VINs_columns_names, header=None)
            print(f"🔓 Safely read from {vin_file_path}")
            file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
            file.writelines(f"🔓 Safely read from {vin_file_path}")
            file.close()
        
        df_VINs.loc[df_VINs['VIN'] == thisVIN, ['CALCULATION']] = ['FN']

    
        with lock:  # This ensures only one job writes at a time
            df_VINs.to_csv(vin_file_path, index = None, mode = 'w', header=False)
            print(f"✔️ Wrote to {vin_file_path} safely with lock.")
            file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/outputs/outputForJob_{the_calculator_jobID_for_thisVIN}.txt", "a")
            file.writelines(f"✔️ Wrote to {vin_file_path} safely with lock.")
            file.close()            

else:
    
    file = open(f"/storage/home/yqf5148/work/volvoPennState/Jobs/errors/error_log_{the_calculator_jobID_for_thisVIN}.txt", "a")
    file.writelines(["The VIN argument is failed to be passed for feature calculations. \n".format(remaining_days)])
    file.close()


# In[ ]:


#         lists = results_for_this_VIN_this_day
#         list_of_res = np.array(pd.concat([pd.Series(x) for x in lists], axis=1)).T.tolist()

#         data = list_of_res[0][0]
#         df_features_for_this_VIN_and_this_dayCount = spark.createDataFrame(data = [data], schema = feature_columns)

#         for i in range(1, int(len(list_of_res))):
#             list_for_thisVIN_and_thisDay = list_of_res[i][0]
#             df_for_thisVIN_and_thisDay = spark.createDataFrame(data = [list_for_thisVIN_and_thisDay], schema = feature_columns)
#             df_features_for_this_VIN_and_this_dayCount = df_features_for_this_VIN_and_this_dayCount.union(df_for_thisVIN_and_thisDay)
#         #end for-loop

#         ndays = df_features_for_this_VIN_and_this_dayCount.count()
#         if ndays > 0:
#             print('we finally write something of length', ndays)
#     #         df_features_for_this_VIN_and_this_dayCount.write.mode("append").format('delta').saveAsTable('df_AMT_features2')   on Databricks we were saving results into a table on database. 
#     #         df_features_for_this_VIN_and_this_dayCount.repartition(1).write.option("header",True).csv(path="./data", mode="append")
#     #         write_csv_with_specific_file_name(sc, df_features_for_this_VIN_and_this_dayCount, "./data", "/resulted_dataset.csv")
#             df_features_for_this_VIN_and_this_dayCount.repartition(1).write.option("header",True).format("csv").mode("append").save("./data/dataset")


# In[ ]:


# # import pandas module
# import pandas as pd
 
# # consider a list
# list1 = ["durga", "ramya", "sravya"]
# list2 = ["java", "php", "mysql"]
# list3 = [67, 89, 65]

# lists = [list1, list2, list3]
# df_of_res = np.array(pd.concat([pd.Series(x) for x in lists], axis=1)).T.tolist()
# print(df_of_res)


# In[10]:


# convert the list into dataframe row by
# using zip()
# data = pd.DataFrame(list(zip(list1, list2, list3)),
#                     columns=['student', 'subject', 'marks'])
 
# display(data)

# df_pop = spark.sql('select * from population')
# [c for c in set(df_pop.toPandas()['VIN'].unique()) if c not in tmp]
# tmp = df_pop.toPandas()['VIN'].unique()

# df = pd.DataFrame({'a': [1, 1, 2], 'b': ['a', 'b', 'c'], 'c': [4, 5, 6]})
# # Normalize using Min/Max Normalization.

# display(df)
# df_num = df.select_dtypes(include='number')
# # selected = [s for s in df.columns if 'a' in s]    #+['VIN']
# # print(df_num.dtypes)
# df_norm = (df_num - df_num.mean()) / (df_num.max() - df_num.min())

# # df = df.toPandas()
# df[df_norm.columns] = df_norm
# display(df)

# df = spark.sql('select * from p2457_faults LIMIT 10')
# pandas_df = df.toPandas()
# pandas_df = pandas_df.replace("NA", None)
# pandas_df = pandas_df.replace(",", ".")
# display(pandas_df)




# statement_in_feature_name = 'ROAD_SPEED'
# df_normalized = normalize_numeric_feature_values(statement_in_feature_name, pandas_df)
# display(df_normalized)

