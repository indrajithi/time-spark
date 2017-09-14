import numpy as np
import pandas as pd
import pylab as pl
import csv

c0 = pd.read_csv('ebcextract_051Chicago.txt_04042016', sep= '|')
    
c02 = pd.read_csv('ebcextract_051Chicago.txt_02082017', sep="|", )
c12 = pd.read_csv('ebcextract_051Chicago.txt_08122017', sep="|", )

c21 = pd.read_csv('ebcextract_051Chicago.txt_08212017', sep="|", )
c22 = pd.read_csv('ebcextract_051Chicago.txt_08222017', sep="|", )
#c23 = pd.read_csv('ebcextract_051Chicago.txt_08232017', sep="|", )
c24 = pd.read_csv('ebcextract_051Chicago.txt_08242017', sep="|", )

c25 = pd.read_csv('25-aug/ebcextract_051Chicago.txt_08252017', sep="|", )
c26 = pd.read_csv('26-aug/ebcextract_051Chicago.txt_08262017', sep="|", )
#c28 = pd.read_csv('28-aug/ebcextract_051Chicago.txt_08282017', sep="|", error_bad_lines=False)
c29 = pd.read_csv('ebcextract_051Chicago.txt_04042016', sep="|" )
c30 = pd.read_csv('ebcextract_051Chicago.txt_08302017', sep="|" )
c31 = pd.read_csv('ebcextract_051Chicago.txt_08312017', sep="|" )






pl.plot(np.arange(len(c22)), c22.QuantityAvailable, color='r', label='c22')
pl.plot(np.arange(len(c23)), c23.QuantityAvailable,color='g', label='c23')
pl.plot(np.arange(len(c24)), c24.QuantityAvailable,color='b', label='c24')


In [179]: c23.QuantityAvailable.describe()
Out[179]: 
count    101177.000000
mean          2.003143
std           6.496466
min           0.000000
25%           0.000000
50%           0.000000
75%           2.000000
max         737.000000
Name: QuantityAvailable, dtype: float64

In [180]: c22.QuantityAvailable.describe()
Out[180]: 
count    101179.000000
mean          2.025895
std           6.581021
min           0.000000
25%           0.000000
50%           0.000000
75%           2.000000
max         737.000000
Name: QuantityAvailable, dtype: float64



In [192]: c22.QuantityAvailable.std() - c23.QuantityAvailable.std()
Out[192]: 0.084554596702425044


In [203]: c22.QuantityAvailable.std() - c26.QuantityAvailable.std()
Out[203]: 0.11732025743857388




# std diff

def stdf(tb1, tb2):
    for id in tb1.describe():
        print(id,":",tb1[id].std() - tb2[id].std())

#find miss
def miss(tb1,tb2):
     count = 0
     for item1 in tb1.Id:
        for item2 in tb2.Id:
            if item1 == item2:
                count += 1
                print(count)
                break
     return count, (len(tb1) - count)

 s[s.isin(['z'])].empty


def match(tb1, tb2):
    miss = 0
    for i in range(len(tb1)):
        if tb1[tb1.isin([tb2.Id[i]])].empty == True: #means no items match
            miss += 1
    return miss

def hit(tb1,tb2):
    ht = 0
    for i in range(len(tb1)):
        if tb1[tb1.isin([tb2.Id[i]])].empty == False: #means items match
            ht += 1
            print(ht)
    return ht


l2 = sc.textFile('filename')
t = l2.map(lambda x: [x.encode('utf8').split("|")[0],x.encode('utf8')[27]])
t = c30.map(lambda x: [x.encode('utf8').split("|")[2],x.encode('utf8')[27]]) 




ebcextract_051Chicago.txt_08252017  ebcextract_051Chicago.txt_08292017  ebcextract_051Chicago.txt_08312017  FTP Downloaded Files.zip
ebcextract_051Chicago.txt_08262017  ebcextract_051Chicago.txt_08302017  ebcextract_051Chicago.txt_09042016 


c25 = pd.read_csv('ebcextract_051Chicago.txt_08252017', sep="|", )
c26 = pd.read_csv('ebcextract_051Chicago.txt_08262017', sep="|", )
c29 = pd.read_csv('ebcextract_051Chicago.txt_08292017', sep="|" )
c30 = pd.read_csv('ebcextract_051Chicago.txt_08302017', sep="|" )
c31 = pd.read_csv('ebcextract_051Chicago.txt_08312017', sep="|" )

#create rdd from pandas
rd1 = sqlContext.createDataFrame(t1)
rd2 = sqlContext.createDataFrame(t2)


#date
import datetime

def to_integer(dt_time):
    return 10000*dt_time.year + 100*dt_time.month + dt_time.day

to_integer(datetime.date.today())

#coustom column date
from pyspark.sql.functions import lit
tdy = to_integer(datetime.date.today())

df.withColumn('new_column', lit(tdy))



from pyspark.sql import SQLContext
from pyspark.sql.types import *

sqlContext = SQLContext(sc)
customSchema = StructType([ \
    StructField("PartNumber", StringType(), True), \
    StructField("QuantityAvailable", StringType(), True)])

df = sqlContext.read \
    .format("com.databricks.spark.csv") \
    .options(header="true") \
    .option("delimiter", "|")
    .load("ebcextract_051Chicago.txt_08252017", schema = customSchema)



sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("delimiter", "|").load("ebcextract_051Chicago.txt_08252017")


In [230]: gt2 = pt2.map(lambda x: (x[0],[x[1],x[2]]))

In [231]: gt2.take(4)
17/09/04 13:27:56 WARN TaskSetManager: Stage 65 contains a task of very large size (608 KB). The maximum recommended task size is 100 KB.
Out[231]: 
[(u'ABPCV31ELS', [5, 20170903]),
 (u'ABPCV31ERS', [0, 20170903]),
 (u'AC1000110', [0, 20170903]),
 (u'AC1000116', [0, 20170903])]


tst = gt1.join(gt2)

In [248]: tst.take(4)
17/09/04 14:18:17 WARN TaskSetManager: Stage 74 contains a task of very large size (585 KB). The maximum recommended task size is 100 KB.
Out[248]:                                                                       
[(u'GM1230358V', ([2, 20170904], [2, 20170903])),
 (u'GM2802106C', ([0, 20170904], [0, 20170903])),
 (u'VW1248135', ([5, 20170904], [5, 20170903])),
 (u'GM1088181', ([4, 20170904], [4, 20170903]))]





We have total of 50GB data at hand. Increasing at the rate 300MB per day

To model


import pandas as pd

filelds = ['PartNumber','QuantityAvailable']
c25 = pd.read_csv('ebcextract_051Chicago.txt_08252017', sep="|", usecols=filelds)
c26 = pd.read_csv('ebcextract_051Chicago.txt_08262017', sep="|", usecols=filelds)
c29 = pd.read_csv('ebcextract_051Chicago.txt_08292017', sep="|", usecols=filelds)
c30 = pd.read_csv('ebcextract_051Chicago.txt_08302017', sep="|", usecols=filelds)
c31 = pd.read_csv('ebcextract_051Chicago.txt_08312017', sep="|", usecols=filelds)


rd1 = sqlContext.createDataFrame(c25)
rd2 = sqlContext.createDataFrame(c26)
rd3 = sqlContext.createDataFrame(c29)
rd4 = sqlContext.createDataFrame(c30)
rd5 = sqlContext.createDataFrame(c31)

#add date column
import datetime

def to_integer(dt_time):
    return 10000*dt_time.year + 100*dt_time.month + dt_time.day

to_integer(datetime.date.today())

#coustom column date
from pyspark.sql.functions import lit
tdy = to_integer(datetime.date.today())

rd1 = rd1.withColumn('date', lit(tdy))
rd2 = rd2.withColumn('date', lit(tdy - 1))
rd2 = rd2.withColumn('date', lit(tdy - 2))
rd3 = rd3.withColumn('date', lit(tdy - 3))
rd4 = rd4.withColumn('date', lit(tdy - 4))
rd5 = rd5.withColumn('date', lit(tdy - 5))

#create key value prair from pipelinedRDD 
p1 = rd1.rdd.map(lambda x: (x[0],[x[1],x[2]]))
p2 = rd2.rdd.map(lambda x: (x[0],[x[1],x[2]]))
p3 = rd3.rdd.map(lambda x: (x[0],[x[1],x[2]]))
p4 = rd4.rdd.map(lambda x: (x[0],[x[1],x[2]]))
p5 = rd5.rdd.map(lambda x: (x[0],[x[1],x[2]]))

    


p1 = rd1.rdd.map(lambda x: (x[0],[x[1]]))
p2 = rd2.rdd.map(lambda x: (x[0],[x[1]]))
p3 = rd2.rdd.map(lambda x: (x[0],[x[1]]))

t2 = t1.map(lambda (x, (a,b)) :(x,(a+b)))

#get values from key
df_acc.lookup('VW1250105')


#find max
t.max(key=lambda x: x[1])

#sort decending
m = t.sortBy(lambda x: x[1], ascending=False)


def toCSVLine(data):
  return ','.join(str(d) for d in data)

lines = labelsAndPredictions.map(toCSVLine)

flattern = lambda a: [item for sublist in a for item in sublist]

In [737]: def grp(pnos):
     ...:     for pno in pnos:
     ...:         a = df_acc.lookup(pno)
     ...:         a = flattern(a)
     ...:         pl.plot(list(range(len(a))),a)
     ...:     pl.show()
     ...:     

change = lambda a: (a[1] - a[0])*100/a[0]


In [60]: def avg(n):
    ...:     pev = []
    ...:     nev = []
    ...:     
    ...:     for i in range(len(n) -1):
    ...:         c = change([n[i], n[i+1]])
    ...:         if c > 0:
    ...:             pev.append(c)
    ...:         if c < 0:
    ...:             nev.append(c)
    ...:     print("pev:", pev, "avg:" ,sum(pev)/len(n) )
    ...:     print("nev:", nev, "avg"  ,sum(nev)/len(n) )
    ...:     
    ...: 
    ...:      

  
#polyfit
         z = np.polyfit(x, y, 3)
    ...: f = np.poly1d(z)
    ...: 
    ...: # calculate new x's and y's
    ...: x_new = np.linspace(x[0], x[-1], 50)
    ...: y_new = f(x_new)
    ...: 
    ...: plt.plot(x,y,'o', x_new, y_new)
    ...: plt.xlim([x[0]-1, x[-1] + 1 ])
    ...: plt.show()

pltt = lambda a: plt.plot(list(range(len(y))), y)

In [140]: yhat =savgol_filter(y,len(x),2)
     ...: plt.plot(x,y)
     ...: plt.plot(x,yhat, color='red')
     ...: plt.show()


#cubic plot
def Multyplot(pnos):
     ...:     count = 0
     ...:     for pno in pnos:
     ...:         y = lk(pno)
     ...:         x = X(y)
     ...:         yhat = savgol_filter(y, len(x)/2 -1 , 3)
     ...:         
     ...:         
     ...:         if count % 4 == 0:
     ...:             #plt.figure(count/4)
     ...:             print count, count%4
     ...:             fig, ax = plt.subplots(nrows=2,ncols=2)
     ...:         plt.subplot(2,2, count %4 + 1 )
     ...:         plt.plot(x,y)
     ...:         plt.plot(x,yhat, color = 'red')
     ...:         count +=1
     ...:     pl.show()


In [382]: def Multyplot2(pnos):
     ...:     count = 0
     ...:     for pno in pnos:
     ...:         y = lk(pno)
     ...:         x = X(y)
     ...:         yhat = savgol_filter(y,7 , 3)
     ...:         
     ...:         
     ...:         if len(pnos) >=4 and count % 4 == 0:
     ...:             #plt.figure(count/4)
     ...:             #print count, count%4
     ...:             fig, ax = plt.subplots(nrows=2,ncols=2)
     ...:         if len(pnos) >= 4:
     ...:             plt.subplot(2,2, count %4 + 1 )
     ...:         if len(pnos) == 2:
     ...:             plt.subplot(2,1, count %2 + 1)
     ...:         if len(pnos) == 3:
     ...:             plt.subplot(2,2, count %4 + 1 )
     ...:         plt.plot(x,y)
     ...:         plt.plot(x,yhat, color = 'red')
     ...:         count +=1
     ...:     pl.show()


getavg = lambda a : [a[i] - a[i+1]  for i in range(len(a) -1)  ]



In [677]: pd = df.rdd.map(lambda x: [x[0],[[x[1]] ,[x[2]]]])

In [678]: pd1 = df1.rdd.map(lambda x: [x[0],[[x[1]] ,[x[2]]]])

In [679]: pd2 = df2.rdd.map(lambda x: [x[0],[[x[1]] ,[x[2]]]])

In [680]: tt = pd1.join(pd2)

In [681]: tt.take(3)
17/09/14 17:22:25 WARN TaskSetManager: Stage 10061 contains a task of very large size (633 KB). The maximum recommended task size is 100 KB.
Out[681]:                                                                       
[(u'GM1230358V',
  ([[0], [u'8/17/2017 9:01:00 PM']], [[0], [u'8/18/2017 9:01:00 PM']])),
 (u'GM2802106C',
  ([[0], [u'8/17/2017 9:01:00 PM']], [[0], [u'8/18/2017 9:00:00 PM']])),
 (u'VW1248135',
  ([[5], [u'8/17/2017 9:01:00 PM']], [[5], [u'8/18/2017 9:01:00 PM']]))]



  d = tt.map(lambda (x,(a,b)): (x,(a[0] + b[0], a[1] + b[1])))