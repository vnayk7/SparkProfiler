import spark_df_profiling
import pyspark.sql.functions
import pandas as pd
import numpy as np
import json
import pickle
from pyspark.sql import SparkSession
from pyspark.sql.functions import (abs as df_abs, col, count, countDistinct,
                                   max as df_max, mean, min as df_min,
                                   sum as df_sum, when
                                   )

spark = SparkSession.builder.appName("run").getOrCreate()
df = spark.read.csv("C:\\Users\\vikum21\\Downloads\\spark-df-profiling\\examples\\export3.csv", inferSchema=True,
                    header=True).limit(20)
# df.show(20, False)
df_desc_acc = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", "~") \
    .option("multiLine", "true") \
    .option("wholeFile", "true") \
    .option("sep", ",") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("encoding", "utf-8") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .load("C:\\Users\\vikum21\\Downloads\\spark-df-profiling\\examples\\col_desc.csv")

df_desc_zi = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", "~") \
    .option("multiLine", "true") \
    .option("wholeFile", "true") \
    .option("sep", "~") \
    .option("ignoreLeadingWhiteSpace", "true") \
    .option("ignoreTrailingWhiteSpace", "true") \
    .option("encoding", "utf-8") \
    .option("quote", "\"") \
    .option("escape", "\n") \
    .load("C:\\Users\\vikum21\\Downloads\\spark-df-profiling_v1\\examples\\DD_New.csv")
# df_desc_zi.toPandas().to_excel(r'C:\\Users\\vikum21\\Downloads\\DD.xlsx',sheet_name='Sheet1', index=False)

# with open('C:\\Users\\vikum21\\Downloads\\gitProjects\\spark-df-profiling-local\\spark_df_profiling\\config\\config.json') as config_file:
#     config = json.load(config_file)
title = "ZoomInfo~UnitedKingdom~November~2022"
[profile, sample] = spark_df_profiling.ProfileGenerate.profilegen(df)
# Store data (serialize)
# with open('C:\\Users\\vikum21\\Downloads\\profile_zi.pickle', 'wb') as handle:
#     pickle.dump(profile, handle, protocol=pickle.HIGHEST_PROTOCOL)

# Load data (deserialize)
# with open('C:\\Users\\vikum21\\Downloads\\spark-df-profiling\\examples\\profile.pickle', 'rb') as handle:
#     unserialized_profile = pickle.load(handle)
# Adding Data Dictionary Description to profile
# Preparing dataframe from variable_stats to be joined same format as df_desc.toPandas()
vstat_t = profile['variables']
# # Adding a new column 'Column' from index of existing DF
vstat_t['Column'] = vstat_t.index
# # Performing the join
variable_stats_desc = pd.merge(vstat_t, df_desc_zi.toPandas(), how='left', left_on=['Column'], right_on=['Column']).T
# # variable_stats_desc.columns=variable_stats.columns
variable_stats_desc.columns = vstat_t.T.columns
profile['variables'] = variable_stats_desc.T
# cols = [x for x in profile['variables'].columns if not x.startswith(("p_","skewness"))]
# profile['variables']=profile['variables'][cols].applymap(lambda x: "{:,}".format(x) if isinstance(x, float)  else x)
sample = df.limit(5).toPandas()
profilereport = spark_df_profiling.ProfileReport(profile, sample)
pTitle = title.split("~")[0] + " " + title.split("~")[1] + " Data Profile " + title.split("~")[2] + "(" + title.split("~")[3] + ")"
profilereport.html = profilereport.html.replace("Title", pTitle)
profilereport.to_file(title, outputfile="C:\\Users\\vikum21\\Downloads\\acc_sample13.html")
