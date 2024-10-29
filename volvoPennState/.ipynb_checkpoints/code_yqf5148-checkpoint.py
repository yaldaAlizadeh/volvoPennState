# df = spark.sql('select * from ds_volvopennstate.p0401_faults where CAST(ds_volvopennstate.p0401_faults.FAULT_DATE_TIME as DATE) > "'"2019-10-01"'" and CAST(ds_volvopennstate.p0401_faults.FAULT_DATE_TIME as DATE) < "'"2020-02-01"'" ') #and  ds_volvopennstate.p0401_faults.VIN=="'"1M2AN09C1GM021196"'"   ')
# display(df)
# df2 = spark.sql('select * from ds_volvopennstate.cca_claims where CAST(ds_volvopennstate.cca_claims.REP_DATE as DATE) > "'"2019-12-01"'" and CAST(ds_volvopennstate.cca_claims.REP_DATE as DATE) < "'"2019-12-10"'" and ds_volvopennstate.cca_claims.TOT_CLAIM_PAYMENT_USD > 1000.0  and ds_volvopennstate.cca_claims.REP_CNTRY == "'"USA"'" ')
# display(df2)   # VIN=4V4N19EHXFN179122 has more than one part replaced in 2019-2020 timespan
# df3 = spark.sql('select * from ds_volvopennstate.egr_cooler_claims where CAST(ds_volvopennstate.egr_cooler_claims.REP_DATE as DATE) > "'"2019-10-01"'" and CAST(ds_volvopennstate.egr_cooler_claims.REP_DATE as DATE) < "'"2020-02-01"'" and ds_volvopennstate.egr_cooler_claims.TOT_CLAIM_PAYMENT_USD > 1000.0  and ds_volvopennstate.egr_cooler_claims.REP_CNTRY == "'"USA"'" ')
# display(df3)   # VIN=4V4N19EHXFN179122 has more than one part replaced in 2019-2020 timespan
# df4 = spark.sql('select * from ds_volvopennstate.egr_fg_293_claims where CAST(ds_volvopennstate.egr_fg_293_claims.REP_DATE as DATE) > "'"2019-10-01"'" and CAST(ds_volvopennstate.egr_fg_293_claims.REP_DATE as DATE) < "'"2020-02-01"'" and ds_volvopennstate.egr_fg_293_claims.TOT_CLAIM_PAYMENT_USD > 1000.0  and ds_volvopennstate.egr_fg_293_claims.REP_CNTRY == "'"USA"'" ')
# display(df4)   # VIN=4V4N19EHXFN179122 has more than one part replaced in 2019-2020 timespan

# df5 = spark.sql('select * from ds_volvopennstate.df_features where ds_volvopennstate.df_features.if_parts_replaced_in_second_15days == 1 ')#ds_volvopennstate.df_features.VIN=="'"1M1AN4GY3KM003995"'" ')
# display(df5)

pip install SQLAlchemy

import pandas as pd
import pyspark as psk
from pyspark.sql import SparkSession
import time as t  
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql import Row
from functools import reduce
from pyspark.sql.types import StructType, StructField, StringType
from datetime import date, datetime, timedelta
from joblib import Parallel, delayed
import multiprocessing
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
import numpy as np
from math import modf

spark = SparkSession.builder.getOrCreate()
sc=spark.sparkContext
  

def partIsReplacedForVINs(df_claims, start_date_str, end_date_str, previous_15day_duration_start_date_str, previous_15day_duration_end_date_str):
    df_temp_1 = df_claims.filter((f.col('CLAIM_REG_DATE') > end_date_str) & (f.col('CLAIM_REG_DATE') < start_date_str) & (f.col('TOT_CLAIM_PAYMENT_USD') > 1000.0))\
                                          .withColumn('part_replacement',f.lit('first_15_days_back'))
    df_temp_2 = df_claims.filter((f.col('CLAIM_REG_DATE') > previous_15day_duration_end_date_str) & (f.col('CLAIM_REG_DATE') < previous_15day_duration_start_date_str) & (f.col('TOT_CLAIM_PAYMENT_USD') > 1000.0))\
                                          .withColumn('part_replacement',f.lit('another_15_days_before_first_15d'))
    
    df_temp = df_temp_1.unionByName(df_temp_2)
    
    df_part_repl = df_temp.groupBy('VIN').pivot('part_replacement',['first_15_days_back','another_15_days_before_first_15d']).count().fillna(0)
    return df_part_repl
  
  
def feature_1_or_2_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, dtc_status, end_date, start_date):
    df_dtc_type_for_this_VIN = spark.sql("select * from ds_volvopennstate.{} where ds_volvopennstate.{}.VIN = '{}' ".format(dtc_type, dtc_type, thisVIN))
    df_dtc_type_and_status_for_this_VIN = df_dtc_type_for_this_VIN\
                .filter(df_dtc_type_for_this_VIN.FAULT_STATUS == dtc_status)\
                .filter((f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') > start_date) & (f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') < end_date))
    return df_dtc_type_and_status_for_this_VIN.count()
  

def feature_3_or_4_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, dtc_status, which_15_days_back, end_date, start_date):
    ''' Step 2: alarms of any type, FAULT_STATUS = 'Y' or 'N' or 'I' '''
    df_dtc_type_for_this_VIN = spark.sql("select * from ds_volvopennstate.{} where ds_volvopennstate.{}.VIN = '{}' ".format(dtc_type, dtc_type, thisVIN))

    df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan = df_dtc_type_for_this_VIN.filter((f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') > start_date) & (f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') < end_date))

    df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan = df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan.withColumn('dtc_alarms', f.concat(f.lit(which_15_days_back), f.lit("_15_days_back"))) 


    my_temp_df = (   
        df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan.alias("a").sort("FAULT_DATE_TIME")\
        .join(
            df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan.alias("b").select('VIN','FAULT_DATE_TIME', 'FAULT_STATUS').sort("FAULT_DATE_TIME"),
            [

                f.to_timestamp(f.col('a.FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') < f.to_timestamp(f.col('b.FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss'), 
                f.col("a.FAULT_STATUS") == f.lit(dtc_status),
                f.col("b.FAULT_STATUS") != f.lit(dtc_status)

            ],
            "INNER",
        )\
      .withColumn("FAULT_DATE_TIME_DIFF", (f.unix_timestamp("b.FAULT_DATE_TIME") - f.unix_timestamp("a.FAULT_DATE_TIME")))\
      .withColumn("DURATION_IN_THIS_15_DAYS_BACK", f.when((f.col('a.FAULT_DATE_TIME') >= start_date) & (f.col('b.FAULT_DATE_TIME') <= end_date), (f.unix_timestamp("b.FAULT_DATE_TIME") - f.unix_timestamp("a.FAULT_DATE_TIME"))))\
      .sort(f.col("b.FAULT_DATE_TIME"))\
      .select(

           f.col("a.FAULT_DATE_TIME").alias("FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC"),
           f.col("b.FAULT_DATE_TIME").alias("FAULT_DATE_TIME_NEXT_ANY_DTC"),
           f.col("FAULT_DATE_TIME_DIFF"),
           f.col("DURATION_IN_THIS_15_DAYS_BACK"),
           f.col("a.VIN").alias("VIN"),
           f.col("a.FAULT_STATUS").alias("FAULT_STATUS_CURRENT_DTC"),
           f.col("b.FAULT_STATUS").alias("FAULT_ANY_STATUS_NEXT_DTC"),


         )
     ).fillna(0).dropDuplicates(['VIN','FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC'])

    duration_of_specific_type_dtcs_for_this_VIN = my_temp_df.select(f.sum("DURATION_IN_THIS_15_DAYS_BACK")).fillna(0)  
    
    return duration_of_specific_type_dtcs_for_this_VIN.collect()[0][0]
  
  
def feature_5_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, dtc_status, which_15_days_back, end_date, start_date):  
    df_dtc_type_for_this_VIN = spark.sql("select * from ds_volvopennstate.{} where ds_volvopennstate.{}.VIN = '{}' ".format(dtc_type, dtc_type, thisVIN))

    df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan = df_dtc_type_for_this_VIN.filter((f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') > start_date) & (f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') < end_date))

    df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan = df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan.withColumn('dtc_alarms', f.concat(f.lit(which_15_days_back), f.lit("_15_days_back"))) 


    my_temp_df = (   
        df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan.alias("a").sort("FAULT_DATE_TIME")\
        .join(
            df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan.alias("b").select('VIN','FAULT_DATE_TIME', 'FAULT_STATUS').sort("FAULT_DATE_TIME"),
            [

                f.to_timestamp(f.col('a.FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') < f.to_timestamp(f.col('b.FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss'), 
                f.col("a.FAULT_STATUS") == f.lit(dtc_status),
                f.col("b.FAULT_STATUS") != f.lit(dtc_status)

            ],
            "INNER",
        )\
      .withColumn("FAULT_DATE_TIME_DIFF", (f.unix_timestamp("b.FAULT_DATE_TIME") - f.unix_timestamp("a.FAULT_DATE_TIME")))\
      .withColumn("DURATION_IN_THIS_15_DAYS_BACK", f.when((f.col('a.FAULT_DATE_TIME') >= start_date) & (f.col('b.FAULT_DATE_TIME') <= end_date), (f.unix_timestamp("b.FAULT_DATE_TIME") - f.unix_timestamp("a.FAULT_DATE_TIME"))))\
      .withColumn("COUNT_IN_THIS_15_DAYS_BACK", f.when((f.col('a.FAULT_DATE_TIME') >= start_date) & (f.col('b.FAULT_DATE_TIME') <= end_date), (f.lit(1)).cast(IntegerType())))\

      .sort(f.col("b.FAULT_DATE_TIME"))\
      .select(

           f.col("a.FAULT_DATE_TIME").alias("FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC"),
           f.col("b.FAULT_DATE_TIME").alias("FAULT_DATE_TIME_NEXT_ANY_DTC"),
           f.col("FAULT_DATE_TIME_DIFF"),
           f.col("DURATION_IN_THIS_15_DAYS_BACK"),
           f.col("COUNT_IN_THIS_15_DAYS_BACK"),
           f.col("a.VIN").alias("VIN"),
           f.col("a.FAULT_STATUS").alias("FAULT_STATUS_CURRENT_DTC"),
           f.col("b.FAULT_STATUS").alias("FAULT_ANY_STATUS_NEXT_DTC"),


         )
     ).fillna(0).dropDuplicates(['VIN','FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC'])

    duration_of_specific_type_dtcs_for_this_VIN = my_temp_df.select(f.sum("DURATION_IN_THIS_15_DAYS_BACK")).fillna(0)
    count_of_specific_type_dtcs_for_this_VIN = my_temp_df.select(f.sum("COUNT_IN_THIS_15_DAYS_BACK")).fillna(1)


    if(count_of_specific_type_dtcs_for_this_VIN.collect()[0][0] != 0):
      average_of_specific_type_dtcs_for_this_VIN = duration_of_specific_type_dtcs_for_this_VIN.collect()[0][0]/count_of_specific_type_dtcs_for_this_VIN.collect()[0][0]

      return average_of_specific_type_dtcs_for_this_VIN
    else:
      return 0
  
  
  
def feature_6_or_7_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, vehicle_speed, dtc_status, end_date, start_date):
    df_dtc_type_for_this_VIN = spark.sql("select * from ds_volvopennstate.{} where ds_volvopennstate.{}.VIN = '{}' ".format(dtc_type, dtc_type, thisVIN))
    df_dtc_type_and_status_for_this_VIN = df_dtc_type_for_this_VIN\
                .filter(df_dtc_type_for_this_VIN.FAULT_STATUS == dtc_status)\
                .filter(df_dtc_type_for_this_VIN.ROAD_SPEED_MPH == vehicle_speed)\
                .filter((f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') > start_date) & (f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') < end_date))
    return df_dtc_type_and_status_for_this_VIN.count()
  
  
def if_part_is_replaced_for_this_VIN_in_this_timespan(thisVIN, start_date, end_date):    
    df_cca_claims = spark.sql("select * from ds_volvopennstate.cca_claims where ds_volvopennstate.cca_claims.VIN = '{}' ".format(thisVIN))
    df_egr_cooler_claims = spark.sql("select * from ds_volvopennstate.egr_cooler_claims where ds_volvopennstate.egr_cooler_claims.VIN = '{}' ".format(thisVIN))
    df_egr_fg_293_claims = spark.sql("select * from ds_volvopennstate.egr_fg_293_claims where ds_volvopennstate.egr_fg_293_claims.VIN = '{}' ".format(thisVIN))
    df_egr_sensors_claims = spark.sql("select * from ds_volvopennstate.egr_sensors where ds_volvopennstate.egr_sensors.VIN = '{}' ".format(thisVIN))

    df_cca_claims_part_replacements = df_cca_claims.filter((f.col('CLAIM_REG_DATE') > start_date) & (f.col('CLAIM_REG_DATE') < end_date) & (f.col('TOT_CLAIM_PAYMENT_USD') > 1000.0))

    df_egr_cooler_claims_part_replacements = df_egr_cooler_claims.filter((f.to_timestamp(f.col('CLAIM_REG_DATE'), 'M/d/yyyy')> start_date) & (f.to_timestamp(f.col('CLAIM_REG_DATE'), 'M/d/yyyy') < end_date) & (f.col('TOT_CLAIM_PAYMENT_USD') > 1000.0))

    df_egr_fg_293_claims_part_replacements = df_egr_fg_293_claims.filter((f.to_timestamp(f.col('CLAIM_REG_DATE'), 'M/d/yyyy')> start_date) & (f.to_timestamp(f.col('CLAIM_REG_DATE'), 'M/d/yyyy') < end_date) & (f.col('TOT_CLAIM_PAYMENT_USD') > 1000.0))
    
    
    df_egr_sensors_claims_part_replacements = df_egr_sensors_claims.filter((f.to_timestamp(f.col('CLAIM_REG_DATE'), 'M/d/yyyy')> start_date) & (f.to_timestamp(f.col('CLAIM_REG_DATE'), 'M/d/yyyy') < end_date) & (f.col('TOT_CLAIM_PAYMENT_USD') > 1000.0))

    number_of_parts_replaced_for_thisVIN = df_cca_claims_part_replacements.count() + df_egr_cooler_claims_part_replacements.count() + df_egr_fg_293_claims_part_replacements.count() + df_egr_sensors_claims_part_replacements.count()
    
    if number_of_parts_replaced_for_thisVIN > 0:
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


      
def move_over_calendar_and_compute_features(thisVIN, new_15day_end_date, span_length, dayCount):
    print("A new day move on calendar: thisVIN={}, new_15day_end_date={}, span_length={}, dayCount={} \n".format(thisVIN, new_15day_end_date, span_length, dayCount)) 
#     schema = StructType([])   ***** This does not work. Creating EmptyRDD does not allow to add further columns later using withColumn  ****
#     df_features_for_this_VIN_and_this_dayCount = sqlContext.createDataFrame(sc.emptyRDD(), schema)
#     df_features_for_this_VIN_and_this_dayCount = df_features_for_this_VIN_and_this_dayCount.withColumn("VIN", f.lit(thisVIN)).withColumn("calendar_day", (f.lit(dayCount)).cast(IntegerType()))

    
    schema = StructType([StructField('VIN', StringType(), True),
                      StructField('calendar_day', IntegerType(), True)])
    data = [
        (thisVIN, dayCount)
      ]
    df_features_for_this_VIN_and_this_dayCount = spark.createDataFrame(data = data,
                                 schema = schema)

    new_15day_start_date = new_15day_end_date - timedelta(days = dayCount)

    previous_15day_duration_end_date = new_15day_start_date
    previous_15day_duration_start_date = previous_15day_duration_end_date - timedelta(days = dayCount)

    
    
        
    list_of_dtc_type = ['p1075_38', 'p1075_75', 'p1075_77', 'p1075_86', 'p1075_92', 'p1075_94', 'p0401_faults', 'p2457_faults']
    
    for i in range(len(list_of_dtc_type)):
        dtc_type = list_of_dtc_type[i]

        #feature 1
        '''
        Definition: number of alerts over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        feature_1_value_first_15days = feature_1_or_2_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, "Y", end_date, start_date)

        feature_1_value_second_15days = feature_1_or_2_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, "Y", previous_15day_duration_end_date, previous_15day_duration_start_date)


        #feature 2
        '''
        Definition: number of intermittent alerts over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        feature_2_value_first_15days = feature_1_or_2_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, "I", end_date, start_date)

        feature_2_value_second_15days = feature_1_or_2_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, "I", previous_15day_duration_end_date, previous_15day_duration_start_date)
        

        #feature 3
        '''
        Definition: duration of active alerts over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        feature_3_value_first_15days = feature_3_or_4_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, "Y", "first", end_date, start_date)

        feature_3_value_second_15days = feature_3_or_4_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, "Y", "second", previous_15day_duration_end_date, previous_15day_duration_start_date)


        #feature 4:
        '''
        Definition: duration of intermittent alerts over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        feature_4_value_first_15days = feature_3_or_4_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, "I", "first", end_date, start_date)

        feature_4_value_second_15days = feature_3_or_4_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, "I", "second", previous_15day_duration_end_date, previous_15day_duration_start_date)


        #feature 5
        '''
        Definition: average time between active alerts over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        feature_5_value_first_15days =  feature_5_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, "Y", "first", end_date, start_date)

        feature_5_value_second_15days = feature_5_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, "Y", "second", previous_15day_duration_end_date, previous_15day_duration_start_date)


        #feature 6
        '''
        Definition: number of active alerts with speed = 0 over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        feature_6_value_first_15days =  feature_6_or_7_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, 0, "Y", end_date, start_date)

        feature_6_value_second_15days = feature_6_or_7_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, 0, "Y", previous_15day_duration_end_date, previous_15day_duration_start_date)


        #feature 7
        '''
        Definition: number of intermittent alerts with speed = 0 over the last (span_length = )15 days and the second #(span_length = )15 days before this period for each VIN for the duration of X year(s) (duration_of_compare)
        '''
        feature_7_value_first_15days =  feature_6_or_7_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, 0, "I", end_date, start_date)

        feature_7_value_second_15days = feature_6_or_7_calculate_for_this_VIN_and_this_timespan(thisVIN, dtc_type, 0, "I", previous_15day_duration_end_date, previous_15day_duration_start_date)

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


        df_features_for_this_VIN_and_this_dayCount = df_features_for_this_VIN_and_this_dayCount.withColumn(feature1Name1, (f.lit(feature_1_value_first_15days)).cast(IntegerType()))\
                                                                                               .withColumn(feature1Name2, (f.lit(feature_1_value_second_15days)).cast(IntegerType()))\
                                                                                               .withColumn(feature2Name1, (f.lit(feature_2_value_first_15days)).cast(IntegerType()))\
                                                                                               .withColumn(feature2Name2, (f.lit(feature_2_value_second_15days)).cast(IntegerType()))\
                                                                                               .withColumn(feature3Name1, (f.lit(feature_3_value_first_15days)).cast(FloatType()))\
                                                                                               .withColumn(feature3Name2, (f.lit(feature_3_value_second_15days)).cast(FloatType()))\
                                                                                               .withColumn(feature4Name1, (f.lit(feature_4_value_first_15days)).cast(IntegerType()))\
                                                                                               .withColumn(feature4Name2, (f.lit(feature_4_value_second_15days)).cast(IntegerType()))\
                                                                                               .withColumn(feature5Name1, (f.lit(feature_5_value_first_15days)).cast(FloatType()))\
                                                                                               .withColumn(feature5Name2, (f.lit(feature_5_value_second_15days)).cast(FloatType()))\
                                                                                               .withColumn(feature6Name1, (f.lit(feature_6_value_first_15days)).cast(IntegerType()))\
                                                                                               .withColumn(feature6Name2, (f.lit(feature_6_value_second_15days)).cast(IntegerType()))\
                                                                                               .withColumn(feature7Name1, (f.lit(feature_7_value_first_15days)).cast(IntegerType()))\
                                                                                               .withColumn(feature7Name2, (f.lit(feature_7_value_second_15days)).cast(IntegerType()))\


        #   for loop finished
        
    df_features_for_this_VIN_and_this_dayCount = df_features_for_this_VIN_and_this_dayCount.withColumn("if_parts_replaced_in_first_15days", (f.lit(if_part_is_replaced_for_this_VIN_in_this_timespan(thisVIN, new_15day_start_date, new_15day_end_date))).cast(IntegerType()))\
  .withColumn("if_parts_replaced_in_second_15days", (f.lit(if_part_is_replaced_for_this_VIN_in_this_timespan(thisVIN, previous_15day_duration_start_date, previous_15day_duration_end_date))).cast(IntegerType()))
        
    return df_features_for_this_VIN_and_this_dayCount.toPandas().iloc[:1].values.tolist()       
  


# import pandas module
import pandas as pd
 
# create dataframe with 3 columns
data = pd.DataFrame({
    "id": [7058, 7059, 7072, 7054],
    "name": ['sravan', 'jyothika', 'harsha', 'ramya'],
    "subjects": ['java', 'python', 'html/php', 'php/js']
}
)
 
# get first row using row position
print(data.iloc[0])
 
print("---------------")
 
# get first row using slice operator
print(data.iloc[:1])
list_obj = data.iloc[:1].values.tolist()
print(list_obj)

# import pandas module
import pandas as pd
 
# consider a list
list1 = ["durga", "ramya", "sravya"]
list2 = ["java", "php", "mysql"]
list3 = [67, 89, 65]

lists = [list1, list2, list3]
df_of_res = np.array(pd.concat([pd.Series(x) for x in lists], axis=1)).T.tolist()
print(df_of_res)
 
# convert the list into dataframe row by
# using zip()
# data = pd.DataFrame(list(zip(list1, list2, list3)),
#                     columns=['student', 'subject', 'marks'])
 
# display(data)

# df_pop = spark.sql('select * from ds_volvopennstate.population')
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

# df = spark.sql('select * from ds_volvopennstate.p2457_faults LIMIT 10')
# pandas_df = df.toPandas()
# pandas_df = pandas_df.replace("NA", None)
# pandas_df = pandas_df.replace(",", ".")
# display(pandas_df)




# statement_in_feature_name = 'ROAD_SPEED'
# df_normalized = normalize_numeric_feature_values(statement_in_feature_name, pandas_df)
# display(df_normalized)

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import *
 
# Create a spark session
spark = SparkSession.builder.appName('Empty_Dataframe').getOrCreate()
 
# Create an empty RDD
emp_RDD = spark.sparkContext.emptyRDD()
 
# Create empty schema
# columns = StructType([])

columns = StructType([StructField('Name',
                                  StringType(), True),
                    StructField('Age',
                                StringType(), True),
                    StructField('Gender',
                                StringType(), True)])
 
# Create an empty RDD with empty schema
data = spark.createDataFrame(data = emp_RDD,
                             schema = columns)
 
# Print the dataframe
print('Dataframe :')
data.show()
 
# Print the schema
print('Schema :')
data.printSchema()

# dtc_type = "p2457_faults"

# thisVIN = "4V5RC9EH0GN937266"

# duration_end_date = '2021-12-31'
# dtc_status = "I"
# # Which_15_dates?
# this = "first"
# vehicle_speed = 0

# day_delta = timedelta(days = 1)
# split_date = duration_end_date.split('-')

# end_date = date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
# start_date = end_date - timedelta(days = 90)

# print("start_date = {}, end_date = {}".format(start_date, end_date))
# list_of_dtc_type = ['p1075_38', 'p1075_75', 'p1075_77', 'p1075_86', 'p1075_92', 'p1075_94', 'p0401_faults', 'p2457_faults']

# # for i in range(len(list_of_dtc_type)):
# #       print(i)
# span_length = 15

# # Create an empty RDD with empty schema
# emp_RDD = sc.emptyRDD()
# # columns = StructType([]) without column name   
# columns = StructType([StructField('VIN', StringType(), True),
#                       StructField('calendar_day', IntegerType(), True),
#                       StructField('feature_1_dtc38_first_15_days', IntegerType(), True),
#                       StructField('feature_1_dtc38_second_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc38_first_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc38_second_15_days', IntegerType(), True),
#                       StructField('feature_3_dtc38_first_15_days', FloatType(), True),
#                       StructField('feature_3_dtc38_second_15_days', FloatType(), True),
#                       StructField('feature_4_dtc38_first_15_days', IntegerType(), True),
#                       StructField('feature_4_dtc38_second_15_days', IntegerType(), True),
#                       StructField('feature_5_dtc38_first_15_days', FloatType(), True),
#                       StructField('feature_5_dtc38_second_15_days', FloatType(), True),
#                       StructField('feature_6_dtc38_first_15_days', IntegerType(), True),
#                       StructField('feature_6_dtc38_second_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc38_first_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc38_second_15_days', IntegerType(), True),
                      
#                       StructField('feature_1_dtc75_first_15_days', IntegerType(), True),
#                       StructField('feature_1_dtc75_second_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc75_first_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc75_second_15_days', IntegerType(), True),
#                       StructField('feature_3_dtc75_first_15_days', FloatType(), True),
#                       StructField('feature_3_dtc75_second_15_days', FloatType(), True),
#                       StructField('feature_4_dtc75_first_15_days', IntegerType(), True),
#                       StructField('feature_4_dtc75_second_15_days', IntegerType(), True),
#                       StructField('feature_5_dtc75_first_15_days', FloatType(), True),
#                       StructField('feature_5_dtc75_second_15_days', FloatType(), True),
#                       StructField('feature_6_dtc75_first_15_days', IntegerType(), True),
#                       StructField('feature_6_dtc75_second_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc75_first_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc75_second_15_days', IntegerType(), True),
                      
#                       StructField('feature_1_dtc77_first_15_days', IntegerType(), True),
#                       StructField('feature_1_dtc77_second_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc77_first_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc77_second_15_days', IntegerType(), True),
#                       StructField('feature_3_dtc77_first_15_days', FloatType(), True),
#                       StructField('feature_3_dtc77_second_15_days', FloatType(), True),
#                       StructField('feature_4_dtc77_first_15_days', IntegerType(), True),
#                       StructField('feature_4_dtc77_second_15_days', IntegerType(), True),
#                       StructField('feature_5_dtc77_first_15_days', FloatType(), True),
#                       StructField('feature_5_dtc77_second_15_days', FloatType(), True),
#                       StructField('feature_6_dtc77_first_15_days', IntegerType(), True),
#                       StructField('feature_6_dtc77_second_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc77_first_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc77_second_15_days', IntegerType(), True),
                      
#                       StructField('feature_1_dtc86_first_15_days', IntegerType(), True),
#                       StructField('feature_1_dtc86_second_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc86_first_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc86_second_15_days', IntegerType(), True),
#                       StructField('feature_3_dtc86_first_15_days', FloatType(), True),
#                       StructField('feature_3_dtc86_second_15_days', FloatType(), True),
#                       StructField('feature_4_dtc86_first_15_days', IntegerType(), True),
#                       StructField('feature_4_dtc86_second_15_days', IntegerType(), True),
#                       StructField('feature_5_dtc86_first_15_days', FloatType(), True),
#                       StructField('feature_5_dtc86_second_15_days', FloatType(), True),
#                       StructField('feature_6_dtc86_first_15_days', IntegerType(), True),
#                       StructField('feature_6_dtc86_second_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc86_first_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc86_second_15_days', IntegerType(), True),
                      
#                       StructField('feature_1_dtc92_first_15_days', IntegerType(), True),
#                       StructField('feature_1_dtc92_second_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc92_first_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc92_second_15_days', IntegerType(), True),
#                       StructField('feature_3_dtc92_first_15_days', FloatType(), True),
#                       StructField('feature_3_dtc92_second_15_days', FloatType(), True),
#                       StructField('feature_4_dtc92_first_15_days', IntegerType(), True),
#                       StructField('feature_4_dtc92_second_15_days', IntegerType(), True),
#                       StructField('feature_5_dtc92_first_15_days', FloatType(), True),
#                       StructField('feature_5_dtc92_second_15_days', FloatType(), True),
#                       StructField('feature_6_dtc92_first_15_days', IntegerType(), True),
#                       StructField('feature_6_dtc92_second_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc92_first_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc92_second_15_days', IntegerType(), True),
                      
#                       StructField('feature_1_dtc94_first_15_days', IntegerType(), True),
#                       StructField('feature_1_dtc94_second_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc94_first_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc94_second_15_days', IntegerType(), True),
#                       StructField('feature_3_dtc94_first_15_days', FloatType(), True),
#                       StructField('feature_3_dtc94_second_15_days', FloatType(), True),
#                       StructField('feature_4_dtc94_first_15_days', IntegerType(), True),
#                       StructField('feature_4_dtc94_second_15_days', IntegerType(), True),
#                       StructField('feature_5_dtc94_first_15_days', FloatType(), True),
#                       StructField('feature_5_dtc94_second_15_days', FloatType(), True),
#                       StructField('feature_6_dtc94_first_15_days', IntegerType(), True),
#                       StructField('feature_6_dtc94_second_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc94_first_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc94_second_15_days', IntegerType(), True),
                      
#                       StructField('feature_1_dtc0401_first_15_days', IntegerType(), True),
#                       StructField('feature_1_dtc0401_second_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc0401_first_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc0401_second_15_days', IntegerType(), True),
#                       StructField('feature_3_dtc0401_first_15_days', FloatType(), True),
#                       StructField('feature_3_dtc0401_second_15_days', FloatType(), True),
#                       StructField('feature_4_dtc0401_first_15_days', IntegerType(), True),
#                       StructField('feature_4_dtc0401_second_15_days', IntegerType(), True),
#                       StructField('feature_5_dtc0401_first_15_days', FloatType(), True),
#                       StructField('feature_5_dtc0401_second_15_days', FloatType(), True),
#                       StructField('feature_6_dtc0401_first_15_days', IntegerType(), True),
#                       StructField('feature_6_dtc0401_second_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc0401_first_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc0401_second_15_days', IntegerType(), True),
                      
#                       StructField('feature_1_dtc2457_first_15_days', IntegerType(), True),
#                       StructField('feature_1_dtc2457_second_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc2457_first_15_days', IntegerType(), True),
#                       StructField('feature_2_dtc2457_second_15_days', IntegerType(), True),
#                       StructField('feature_3_dtc2457_first_15_days', FloatType(), True),
#                       StructField('feature_3_dtc2457_second_15_days', FloatType(), True),
#                       StructField('feature_4_dtc2457_first_15_days', IntegerType(), True),
#                       StructField('feature_4_dtc2457_second_15_days', IntegerType(), True),
#                       StructField('feature_5_dtc2457_first_15_days', FloatType(), True),
#                       StructField('feature_5_dtc2457_second_15_days', FloatType(), True),
#                       StructField('feature_6_dtc2457_first_15_days', IntegerType(), True),
#                       StructField('feature_6_dtc2457_second_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc2457_first_15_days', IntegerType(), True),
#                       StructField('feature_7_dtc2457_second_15_days', IntegerType(), True),
                      
#                       StructField('if_parts_replaced_in_first_15days', IntegerType(), True),
#                       StructField('if_parts_replaced_in_second_15days', IntegerType(), True)])
# df_features = spark.createDataFrame(data=emp_RDD, schema=columns)

# # Print the dataframe
# # print('Dataframe :')
# # df_features.show()
 
# # Print the schema
# print('Schema :')
# df_features.printSchema()


# # data = [{'VIN':thisVIN, 'calendar_day':1}]
# # rdd = spark.sparkContext.parallelize(data)
# # df_features = rdd.toDF(columns)
# # display(df_features)



# df_dtc_type_for_this_VIN = spark.sql("select * from ds_volvopennstate.{} ".format(dtc_type))
# display(df_dtc_type_for_this_VIN)


# # feature 1 and 2:
# # df_dtc_type_for_this_VIN = spark.sql("select * from ds_volvopennstate.{} where ds_volvopennstate.{}.VIN = '{}' ".format(dtc_type, dtc_type, thisVIN))
# # df_dtc_type_and_status_for_this_VIN = df_dtc_type_for_this_VIN\
# #             .filter(df_dtc_type_for_this_VIN.FAULT_STATUS == dtc_status)\
# #             .filter((f.to_timestamp(f.col('FAULT_DATE_TIME'), 'M/d/yyyy HH:mm') > new_15day_start_date) & (f.to_timestamp(f.col('FAULT_DATE_TIME'), 'M/d/yyyy HH:mm') < new_15day_end_date))

# # print("The count of {} dtc type of {} status for {} is: {} ".format(dtc_type, dtc_status, thisVIN, df_dtc_type_and_status_for_this_VIN.count()))





# #feature 3 and 4
# # df_dtc_type_for_this_VIN = spark.sql("select * from ds_volvopennstate.{} where ds_volvopennstate.{}.VIN = '{}' ".format(dtc_type, dtc_type, thisVIN))
# # display(df_dtc_type_for_this_VIN)   

# # df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan = df_dtc_type_for_this_VIN.filter((f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') > start_date) & (f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') < end_date))
  
# # df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan = df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan.withColumn('dtc_alarms', f.concat(f.lit(this), f.lit("_15_days_back"))) 
# # display(df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan)


# # my_temp_df = (   
# #     df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan.alias("a").sort("FAULT_DATE_TIME")\
# #     .join(
# #         df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan.alias("b").select('VIN','FAULT_DATE_TIME', 'FAULT_STATUS').sort("FAULT_DATE_TIME"),
# #         [

# #             f.to_timestamp(f.col('a.FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') < f.to_timestamp(f.col('b.FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss'), 
# #             f.col("a.FAULT_STATUS") == f.lit(dtc_status),
# #             f.col("b.FAULT_STATUS") != f.lit(dtc_status)

# #         ],
# #         "INNER",
# #     )\
# #   .withColumn("FAULT_DATE_TIME_DIFF", (f.unix_timestamp("b.FAULT_DATE_TIME") - f.unix_timestamp("a.FAULT_DATE_TIME")))\
# #   .withColumn("DURATION_IN_THIS_15_DAYS_BACK", f.when((f.col('a.FAULT_DATE_TIME') >= start_date) & (f.col('b.FAULT_DATE_TIME') <= end_date), (f.unix_timestamp("b.FAULT_DATE_TIME") - f.unix_timestamp("a.FAULT_DATE_TIME"))))\
# #   .sort(f.col("b.FAULT_DATE_TIME"))\
# #   .select(

# #        f.col("a.FAULT_DATE_TIME").alias("FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC"),
# #        f.col("b.FAULT_DATE_TIME").alias("FAULT_DATE_TIME_NEXT_ANY_DTC"),
# #        f.col("FAULT_DATE_TIME_DIFF"),
# #        f.col("DURATION_IN_THIS_15_DAYS_BACK"),
# #        f.col("a.VIN").alias("VIN"),
# #        f.col("a.FAULT_STATUS").alias("FAULT_STATUS_CURRENT_DTC"),
# #        f.col("b.FAULT_STATUS").alias("FAULT_ANY_STATUS_NEXT_DTC"),


# #      )
# #  ).fillna(0).dropDuplicates(['VIN','FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC'])
# # display(my_temp_df)
# # duration_of_specific_type_dtcs_for_this_VIN = my_temp_df.select(f.sum("DURATION_IN_THIS_15_DAYS_BACK")).fillna(0)
# # display(duration_of_specific_type_dtcs_for_this_VIN)
# # print("The duration of {} dtc type  of {} status for {} is: {} ms".format(dtc_type, dtc_status, thisVIN, duration_of_specific_type_dtcs_for_this_VIN.collect()[0][0]))


# #feature 5:

# # for i in range(len(list_of_dtc_type)):
# #     dtc_type = list_of_dtc_type[i]
# #     df_dtc_type_for_this_VIN = spark.sql("select * from ds_volvopennstate.{} where ds_volvopennstate.{}.VIN = '{}' ".format(dtc_type, dtc_type, thisVIN))
# # #     display(df_dtc_type_for_this_VIN)   

# #     df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan = df_dtc_type_for_this_VIN.filter((f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') > start_date) & (f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') < end_date))

# #     df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan = df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan.withColumn('dtc_alarms', f.concat(f.lit(this), f.lit("_15_days_back"))) 
# # #     display(df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan)


# #     my_temp_df = (   
# #         df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan.alias("a").sort("FAULT_DATE_TIME")\
# #         .join(
# #             df_dtc_type_for_this_VIN_any_dtc_status_in_this_timespan.alias("b").select('VIN','FAULT_DATE_TIME', 'FAULT_STATUS').sort("FAULT_DATE_TIME"),
# #             [

# #                 f.to_timestamp(f.col('a.FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') < f.to_timestamp(f.col('b.FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss'), 
# #                 f.col("a.FAULT_STATUS") == f.lit(dtc_status),
# #                 f.col("b.FAULT_STATUS") != f.lit(dtc_status)

# #             ],
# #             "INNER",
# #         )\
# #       .withColumn("FAULT_DATE_TIME_DIFF", (f.unix_timestamp("b.FAULT_DATE_TIME") - f.unix_timestamp("a.FAULT_DATE_TIME")))\
# #       .withColumn("DURATION_IN_THIS_15_DAYS_BACK", f.when((f.col('a.FAULT_DATE_TIME') >= start_date) & (f.col('b.FAULT_DATE_TIME') <= end_date), (f.unix_timestamp("b.FAULT_DATE_TIME") - f.unix_timestamp("a.FAULT_DATE_TIME"))))\
# #       .withColumn("COUNT_IN_THIS_15_DAYS_BACK", f.when((f.col('a.FAULT_DATE_TIME') >= start_date) & (f.col('b.FAULT_DATE_TIME') <= end_date), (f.lit(1)).cast(IntegerType())))\

# #       .sort(f.col("b.FAULT_DATE_TIME"))\
# #       .select(

# #            f.col("a.FAULT_DATE_TIME").alias("FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC"),
# #            f.col("b.FAULT_DATE_TIME").alias("FAULT_DATE_TIME_NEXT_ANY_DTC"),
# #            f.col("FAULT_DATE_TIME_DIFF"),
# #            f.col("DURATION_IN_THIS_15_DAYS_BACK"),
# #            f.col("COUNT_IN_THIS_15_DAYS_BACK"),
# #            f.col("a.VIN").alias("VIN"),
# #            f.col("a.FAULT_STATUS").alias("FAULT_STATUS_CURRENT_DTC"),
# #            f.col("b.FAULT_STATUS").alias("FAULT_ANY_STATUS_NEXT_DTC"),


# #          )
# #      ).fillna(0).dropDuplicates(['VIN','FAULT_DATE_TIME_CURRENT_SPECIFIC_TYPE_DTC'])
# # #     display(my_temp_df)

# #     duration_of_specific_type_dtcs_for_this_VIN = my_temp_df.select(f.sum("DURATION_IN_THIS_15_DAYS_BACK")).fillna(0)
# #     count_of_specific_type_dtcs_for_this_VIN = my_temp_df.select(f.sum("COUNT_IN_THIS_15_DAYS_BACK")).fillna(1)
# # #     print(count_of_specific_type_dtcs_for_this_VIN.collect()[0][0])


# #     if(count_of_specific_type_dtcs_for_this_VIN.collect()[0][0] != 0):
# #       average_of_specific_type_dtcs_for_this_VIN = duration_of_specific_type_dtcs_for_this_VIN.collect()[0][0]/count_of_specific_type_dtcs_for_this_VIN.collect()[0][0]
# #       print("The average of {} dtc type of {} status for {} is: {} ms".format(dtc_type, dtc_status, thisVIN, average_of_specific_type_dtcs_for_this_VIN))
# #     else:
# #       print("0")


# # feature 6 and 7:
# # list_of_dtc_type = ['p1075_38', 'p1075_75', 'p1075_77', 'p1075_86', 'p1075_92', 'p1075_94', 'p0401_faults', 'p2457_faults']
# # for i in range(len(list_of_dtc_type)):
# #     dtc_type = list_of_dtc_type[i]
# #     df_dtc_type_for_this_VIN = spark.sql("select * from ds_volvopennstate.{} where ds_volvopennstate.{}.VIN = '{}' ".format(dtc_type, dtc_type, thisVIN))
# #     df_dtc_type_and_status_for_this_VIN = df_dtc_type_for_this_VIN\
# #                 .filter(df_dtc_type_for_this_VIN.FAULT_STATUS == dtc_status)\
# #                 .filter(df_dtc_type_for_this_VIN.ROAD_SPEED_MPH == vehicle_speed)\
# #                 .filter((f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') > start_date) & (f.to_timestamp(f.col('FAULT_DATE_TIME'), 'yyyy-MM-dd HH:mm:ss') < end_date))

# #     print("The count of {} dtc type of {} status for {} in the stopped situation is: {} ".format(dtc_type, dtc_status, thisVIN, df_dtc_type_and_status_for_this_VIN.count()))


# tmp_df_for_this_VIN_this_day = Parallel(n_jobs=1)(delayed(move_over_calendar_and_compute_features)(thisVIN, end_date, span_length, day_count) for day_count in range((end_date - start_date).days))
# df_features.union(tmp_df_for_this_VIN_this_day)
# ndays = df_features.count()
#     if ndays > 0:
#         print('we finally write something of length', ndays)
#         df_features.write.mode("append").format('delta').saveAsTable('ds_volvopennstate.df_features')
# display(df_features)


  
# df_cca_claims = spark.sql("select * from ds_volvopennstate.cca_claims where ds_volvopennstate.cca_claims.VIN = '{}' ".format(thisVIN))
# df_egr_cooler_claims = spark.sql("select * from ds_volvopennstate.egr_cooler_claims where ds_volvopennstate.egr_cooler_claims.VIN = '{}' ".format(thisVIN))
# df_egr_fg_293_claims = spark.sql("select * from ds_volvopennstate.egr_fg_293_claims where ds_volvopennstate.egr_fg_293_claims.VIN = '{}' ".format(thisVIN))


# df_cca_claims = spark.sql("select * from ds_volvopennstate.cca_claims ")
# df_egr_cooler_claims = spark.sql("select * from ds_volvopennstate.egr_cooler_claims ")
# df_egr_fg_293_claims = spark.sql("select * from ds_volvopennstate.egr_fg_293_claims ")
    
# df_cca_claims_part_replacements = df_cca_claims.filter((f.col('CLAIM_REG_DATE') > start_date) & (f.col('CLAIM_REG_DATE') < end_date) & (f.col('TOT_CLAIM_PAYMENT_USD') > 1000.0))
# display(df_cca_claims_part_replacements) 
                                 
# df_egr_cooler_claims_part_replacements = df_egr_cooler_claims.filter((f.to_timestamp(f.col('CLAIM_REG_DATE'), 'M/d/yyyy')> start_date) & (f.to_timestamp(f.col('CLAIM_REG_DATE'), 'M/d/yyyy') < end_date) & (f.col('TOT_CLAIM_PAYMENT_USD') > 1000.0))
# display(df_egr_cooler_claims_part_replacements)
                                 
# df_egr_fg_293_claims_part_replacements = df_egr_fg_293_claims.filter((f.to_timestamp(f.col('CLAIM_REG_DATE'), 'M/d/yyyy')> start_date) & (f.to_timestamp(f.col('CLAIM_REG_DATE'), 'M/d/yyyy') < end_date) & (f.col('TOT_CLAIM_PAYMENT_USD') > 1000.0))
# display(df_egr_fg_293_claims_part_replacements)

# part_replacement = df_cca_claims_part_replacements.count() + df_egr_cooler_claims_part_replacements.count() + df_egr_fg_293_claims_part_replacements.count()
# print(part_replacement)

duration_end_date = '2021-12-31'

day_delta = timedelta(days = 1)
split_date = duration_end_date.split('-')

end_date = date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
start_date = end_date - timedelta(days = 1826)

print("start_date = {}, end_date = {}".format(start_date, end_date))

span_length = 15

# Create an empty RDD with empty schema
emp_RDD = sc.emptyRDD()
# columns = StructType([]) without column name   
feature_columns = StructType([StructField('VIN', StringType(), True),
                      StructField('calendar_day', IntegerType(), True),
                      StructField('feature_1_dtc38_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc38_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc38_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc38_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc38_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc38_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc38_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc38_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc38_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc38_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc38_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc38_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc38_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc38_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc75_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc75_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc75_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc75_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc75_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc75_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc75_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc75_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc75_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc75_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc75_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc75_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc75_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc75_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc77_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc77_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc77_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc77_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc77_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc77_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc77_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc77_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc77_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc77_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc77_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc77_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc77_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc77_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc86_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc86_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc86_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc86_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc86_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc86_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc86_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc86_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc86_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc86_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc86_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc86_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc86_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc86_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc92_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc92_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc92_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc92_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc92_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc92_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc92_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc92_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc92_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc92_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc92_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc92_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc92_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc92_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc94_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc94_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc94_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc94_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc94_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc94_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc94_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc94_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc94_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc94_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc94_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc94_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc94_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc94_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc0401_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc0401_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc0401_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc0401_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc0401_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc0401_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc0401_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc0401_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc0401_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc0401_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc0401_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc0401_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc0401_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc0401_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc2457_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc2457_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc2457_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc2457_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc2457_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc2457_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc2457_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc2457_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc2457_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc2457_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc2457_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc2457_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc2457_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc2457_second_15_days', IntegerType(), True),
                      
                      StructField('if_parts_replaced_in_first_15days', IntegerType(), True),
                      StructField('if_parts_replaced_in_second_15days', IntegerType(), True)])

df_features = spark.createDataFrame(data=emp_RDD, schema=feature_columns)


population_num_of_rows = ' LIMIT 1 '
# # df_pop = spark.sql('select * from ds_volvopennstate.population where CAST(ds_volvopennstate.population.ENGINE_ASSEMBLY_DATE as DATE) < "'"2014-01-01"'"  {0} '.format(population_num_of_rows))
# df_pop_VINs = spark.sql('select distinct ds_volvopennstate.population.VIN from ds_volvopennstate.population {0} '.format(population_num_of_rows))
df_pop_VINs = spark.sql('select distinct ds_volvopennstate.population.VIN from ds_volvopennstate.population where 1<2 or ds_volvopennstate.population.VIN == "'"1M1AN07Y7GM025143"'" {0} '.format(population_num_of_rows))   #  All rows in population table is: 322178
# display(df_pop_VINs)
df_pop_VINs_p = df_pop_VINs.toPandas()


for index, row in df_pop_VINs_p.iterrows():
    thisVIN = row['VIN']
    print("Current VIN: {}".format(thisVIN))    
    results_for_this_VIN_this_day = Parallel(n_jobs=multiprocessing.cpu_count(), prefer="threads", batch_size=100)(delayed(move_over_calendar_and_compute_features)(thisVIN, end_date, span_length, day_count) for day_count in range(int((end_date - start_date).days)))   

    lists = results_for_this_VIN_this_day
    list_of_res = np.array(pd.concat([pd.Series(x) for x in lists], axis=1)).T.tolist()
    
    data = list_of_res[0][0]
    df_features_for_this_VIN_and_this_dayCount = spark.createDataFrame(data = [data], schema = feature_columns)

    for i in range(1, int(len(list_of_res))):
        list_for_thisVIN_and_thisDay = list_of_res[i][0]
        df_for_thisVIN_and_thisDay = spark.createDataFrame(data = [list_for_thisVIN_and_thisDay], schema = feature_columns)
        df_features_for_this_VIN_and_this_dayCount = df_features_for_this_VIN_and_this_dayCount.union(df_for_thisVIN_and_thisDay)
    #end for-loop

    ndays = df_features_for_this_VIN_and_this_dayCount.count()
    if ndays > 0:
        print('we finally write something of length', ndays)
        df_features_for_this_VIN_and_this_dayCount.write.mode("append").format('delta').saveAsTable('ds_volvopennstate.df_AMT_features2')
        display(spark.table('ds_volvopennstate.df_AMT_features2'))
            

#end for-loop 

lists = [results_for_this_VIN_this_day]

# print(results_for_this_VIN_this_day)
print(len(results_for_this_VIN_this_day))
# print((results_for_this_VIN_this_day[1]))

feature_columns = StructType([StructField('VIN', StringType(), True),
                      StructField('calendar_day', IntegerType(), True),
                              
                      StructField('feature_1_dtc38_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc38_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc38_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc38_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc38_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc38_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc38_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc38_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc38_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc38_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc38_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc38_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc38_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc38_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc75_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc75_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc75_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc75_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc75_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc75_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc75_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc75_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc75_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc75_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc75_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc75_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc75_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc75_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc77_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc77_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc77_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc77_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc77_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc77_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc77_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc77_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc77_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc77_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc77_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc77_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc77_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc77_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc86_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc86_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc86_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc86_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc86_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc86_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc86_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc86_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc86_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc86_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc86_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc86_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc86_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc86_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc92_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc92_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc92_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc92_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc92_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc92_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc92_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc92_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc92_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc92_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc92_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc92_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc92_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc92_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc94_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc94_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc94_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc94_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc94_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc94_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc94_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc94_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc94_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc94_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc94_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc94_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc94_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc94_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc0401_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc0401_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc0401_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc0401_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc0401_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc0401_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc0401_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc0401_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc0401_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc0401_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc0401_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc0401_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc0401_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc0401_second_15_days', IntegerType(), True),
                      
                      StructField('feature_1_dtc2457_first_15_days', IntegerType(), True),
                      StructField('feature_1_dtc2457_second_15_days', IntegerType(), True),
                      StructField('feature_2_dtc2457_first_15_days', IntegerType(), True),
                      StructField('feature_2_dtc2457_second_15_days', IntegerType(), True),
                      StructField('feature_3_dtc2457_first_15_days', FloatType(), True),
                      StructField('feature_3_dtc2457_second_15_days', FloatType(), True),
                      StructField('feature_4_dtc2457_first_15_days', IntegerType(), True),
                      StructField('feature_4_dtc2457_second_15_days', IntegerType(), True),
                      StructField('feature_5_dtc2457_first_15_days', FloatType(), True),
                      StructField('feature_5_dtc2457_second_15_days', FloatType(), True),
                      StructField('feature_6_dtc2457_first_15_days', IntegerType(), True),
                      StructField('feature_6_dtc2457_second_15_days', IntegerType(), True),
                      StructField('feature_7_dtc2457_first_15_days', IntegerType(), True),
                      StructField('feature_7_dtc2457_second_15_days', IntegerType(), True),
                      
                      
                      StructField('if_parts_replaced_in_first_15days', IntegerType(), True),
                      StructField('if_parts_replaced_in_second_15days', IntegerType(), True)])
lists = results_for_this_VIN_this_day
list_of_res = np.array(pd.concat([pd.Series(x) for x in lists], axis=1)).T.tolist()


# print(range(1, int(len(list_of_res)-1)))
data = list_of_res[0][0]
df_features_for_this_VIN_and_this_dayCount = spark.createDataFrame(data = [data], schema = feature_columns)

for i in range(1, int(len(list_of_res))):
    list_for_thisVIN_and_thisDay = list_of_res[i][0]
    df_for_thisVIN_and_thisDay = spark.createDataFrame(data = [list_for_thisVIN_and_thisDay], schema = feature_columns)
    df_features_for_this_VIN_and_this_dayCount = df_features_for_this_VIN_and_this_dayCount.union(df_for_thisVIN_and_thisDay)

display(df_features_for_this_VIN_and_this_dayCount)

ndays = df_features_for_this_VIN_and_this_dayCount.count()
if ndays > 0:
    print('we finally write something of length', ndays)
    df_features_for_this_VIN_and_this_dayCount.write.mode("append").format('delta').saveAsTable('ds_volvopennstate.df_features')

display(spark.table('ds_volvopennstate.df_features'))

# import multiprocessing as mp
# import random
# from multiprocessing import Pool

# def select_VINs(df_pop):
#     df_temp = df_pop.toPandas()['VIN'].unique()
#     return df_temp


# def parallelize_dataframe(df, func, n_cores=4):
#     df_split = np.array_split(df, n_cores)
#     pool = Pool(n_cores)
#     df = pd.concat(pool.map(func, df_split))
#     pool.close()
#     pool.join()
#     return df
  
  
