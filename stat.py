import numpy as np
import pandas as pd
import pylab as pl
import csv
import glob
import os
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import statistics as st

file_dir    = "chicago"
file_path   = os.path.join(".",file_dir)
files       = glob.glob(file_path + "/*")
files.sort()

filelds = ['PartNumber','QuantityAvailable']

first_flag = True

for file_no in range(7,14):
    
    #load csv in pandas and create dataframe
    f1 = pd.read_csv(files[ file_no ], sep="|", usecols=filelds)
    df = sqlContext.createDataFrame( f1 )
    
    #create (key,value) pair as (part_number, quantity_available)
    pair_df = df.rdd.map(lambda x: (x[0],[x[1]]))

    #execute after first iteration
    if first_flag == False:
        #join pair_df with the old_df_acc
        df_acc = old_df_acc.join( pair_df )
        #flatten the values
        df_acc = df_acc.map(lambda (x, (a,b)):(x,(a+b)))

        old_df_acc = df_acc

    else:
        old_df_acc =  pair_df
        first_flag = False


stdev = df_acc.map(lambda (x, y): (x, st.stdev(y)))

#all the procucts that have standard deviation of 1
stdOf1 = stdev.filter(lambda (x,y): y>1.0)

stdev.filter(lambda (x,y): y>0.5).count()
