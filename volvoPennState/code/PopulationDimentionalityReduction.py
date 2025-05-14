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
import joblib
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
# import covalent as ct

import subprocess
import re
import random

from sklearn.preprocessing import OneHotEncoder
from sklearn.decomposition import PCA
# import umap.umap_ as umap
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

import matplotlib.pyplot as plt

import gc  # import gorbage collector to resolve the problem of restarting kernel due to large table of population loading in RAM to append
from pyspark.sql.functions import year, col

findspark.init()
findspark.find()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# os.environ['PYDEVD_DISABLE_FILE_VALIDATION']=1

spark = SparkSession.builder        .config("spark.ui.port", "4050")        .master("local[2]")        .appName("MyApp")        .config("spark.driver.maxResultSize", "40g")       .config("spark.driver.memory", "140g")       .getOrCreate()
sc = spark.sparkContext

# sqlContext = SQLContext(sc)


#both works
# 1: 
# spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")    #To resolve the error for p1075_38 to_timestamp formating: You may get a different result due to the upgrading to Spark >= 3.0: Fail to parse '1/2/2019 20:40:00' in the new parser. You can set spark.sql.legacy.timeParserPolicy to LEGACY to restore the behavior before Spark 3.0, or set to CORRECTED and treat it as an invalid datetime string.
# Set Spark SQL legacy time parser policy to LEGACY to handle older date formats
# 2:
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# Increase the max fields in the string representation of a plan
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
spark.conf.set("spark.sql.debug.maxToStringFields", 2000)


# In[3]:


spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/data/dataset/VINs_data(problem).csv")           .createOrReplaceTempView("VINs_data")

spark.read.option("header",True)           .csv("/storage/home/yqf5148/work/volvoPennState/PopulationWithChassisId.csv")           .createOrReplaceTempView("population")


# In[4]:


# Get all column names from the population table
all_columns = spark.table("population").columns

# Select only columns that contain "KOLA" and exclude "CHASSIS_ID"
kola_columns = [c for c in all_columns if "KOLA" in c and c != "CHASSIS_ID"]

# Build the main DataFrame with selected columns
df_population = spark.table("population")     .select(
        col("VIN"),       
        col("ENGINE_SIZE"),
        col("ENGINE_HP"),
        col("VEH_TYPE"),
        year("VEH_ASSEMB_DATE").alias("VEH_ASSEMB_YEAR"),
        
        *[col(c) for c in kola_columns]           # Include all KOLA-related columns
    )


# In[ ]:


selected_for_pca_cols = [c for c in df_population.columns if "VIN" not in c]
not_selected_for_pca_cols = [c for c in df_population.columns if c not in selected_for_pca_cols]

# print(non_kola_cols)
df_non_selected_for_pca = df_population.select(*not_selected_for_pca_cols)
df_selected_for_pca = df_population.select(*selected_for_pca_cols)

df_selected_for_pca_pd = df_selected_for_pca.toPandas()  # convert from PySpark to Pandas
print(f"The toPandas() finished!")


# In[ ]:


df_selected_for_pca_pd = df_selected_for_pca_pd.astype('category')
print(f"The convert to category finished!")


# In[7]:


encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
selected_for_pca_encoded = encoder.fit_transform(df_selected_for_pca_pd)
print(f"The OneHotEncoding finished!")

pca = PCA(n_components=0.95)
pca_applied = pca.fit_transform(selected_for_pca_encoded)
print(f"The PCA application on kola columns finished!")


# In[8]:


pca_columns = [f"PCA_{i+1}" for i in range(pca_applied.shape[1])]
df_pca_applied = pd.DataFrame(pca_applied, columns=pca_columns, index=df_selected_for_pca_pd.index)


# In[9]:


df_non_selected_for_pca_pd = df_non_selected_for_pca.toPandas()
df_final = pd.concat([df_non_selected_for_pca_pd.reset_index(drop=True), df_pca_applied], axis=1)


# In[10]:


df_final.to_csv("/storage/home/yqf5148/work/volvoPennState/data/dataset/final_features_with_pca.csv", index=False)


# In[ ]:




