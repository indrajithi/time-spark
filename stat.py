import numpy as np
import pandas as pd
import csv
import glob
import os
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import statistics as st
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

file_dir    = "LKQ/aug/dallas"
file_path   = os.path.join(".",file_dir)
files       = glob.glob(file_path + "/*")
files.sort()
cwd = os.getcwd()
filelds = ['PartNumber','QuantityAvailable','DateCreated','CustomerPrice']


def Sale(a):
    """
    Finds the total units sold in the period
    """
    sale = 0
    for i in range(len(a) -1 ):
        if a[i+1] < a[i]:
            sale += a[i] - a[i + 1 ]
    return sale

def Replenish(a):
    """
    Finds the total units replinished in the period
    """
    repl = 0
    for i in range(len(a) -1 ):
        if a[i+1] > a[i]:
            repl += a[i + 1] - a[i]
    return repl

def Analysis(a):
    sale , repl = Sale(a), Replenish(a)
    if (a[0] + repl ) != 0:
        percentage_sale = (float(sale) / (a[0] + repl )) *100 
    else: percentage_sale = 0

    return sale, repl, round(percentage_sale, 2)

first_flag = True
for file_no in range(0,7):
    
    #load csv in pandas and create dataframe
    f1 = pd.read_csv(files[ file_no ], sep="|", usecols=filelds)
    df = sqlContext.createDataFrame( f1 )
    
    #create (key,value) pair as (part_number, quantity_available, date_created)
    pair_df = df.rdd.map(lambda x: [x[0], [ [x[1]] , [x[2]] , [x[3]] ]])

    #execute after first iteration
    if first_flag == False:
        #join pair_df with the old_df_acc
        df_acc = old_df_acc.join( pair_df )
        #flatten the values
        df_acc = df_acc.map(lambda (x,(a,b)): \
            (x,(a[0] + b[0], a[1] + b[1], a[2] + b[2])))

        old_df_acc = df_acc

    else:
        old_df_acc =  pair_df
        first_flag = False


res = df_acc.map(lambda (x, y): (x, Analysis(y[1]))) \
.map(lambda (x,(a,b,c)): (x,[a]+[b]+[c]))

#res = df_acc.map(lambda (x, y): (x, round(st.stdev(y),2), Analysis(y))) \
#.map(lambda (x,y,(a,b,c)): (x,[y]+[a]+[b]+[c]))


#sorted by maximum sale
maximum_sold = res.sortBy(lambda (x,y): y[0] , ascending=False)

#write = maximum_sold.map(lambda (x,y): [x]+y)
#filter products with atleast one sale and map from (k,(v1,v2,v3)) to [k,v1,v2,v3] 
write = maximum_sold.filter(lambda (x,y): y[0]>=1).map(lambda (x,y): [x] +y)

#Convert RDD to DataFrame
df = sqlContext.createDataFrame(write, ['PartNumber', 'Sale','Replenish','PercentageSale'])

#save dataframe as csv
df.coalesce(1).write.format('com.databricks.spark.csv').options(header='true').save(cwd+'cv_out')

#all the procucts that have standard deviation of 1
stdOf1 = stdev.filter(lambda (x,y): y[0]>1.0)

stdev.filter(lambda (x,y): y>0.5).count()

#sale in range
t = res.filter(lambda (x,y): y[0]>10 and y[0] <50)



#ploting

d = maximum_sold.map(lambda (a,b): a)
#flattern the array [[a,b,c],[d]] to [a,b,c,d]
flattern = lambda a: [item for sublist in a for item in sublist]

def grp(pnos):
    for pno in pnos:
        a = df_acc.lookup(pno)
        a = flattern(a)
        pl.plot(list(range(len(a))),a)
    pl.show()

grp(d.take(10))

#generate x values depending on the length of Y
X = lambda a: np.arange(len(a))

#savitzky_golay window smooting
look = lambda a: np.array(flattern(df_acc.lookup(a)))

yhat = savitzky_golay(y, 21, 3) # window size 51, polynomial order 3

plt.plot(x,y)
plt.plot(x,yhat, color='red')
plt.show()


class Analysis:
    """Class for rate of change, ploting and other analysis
    Attributes: 
                values: list of inventory levels for the time period 
    """
    def __init__(self, pno=None):
        
        self.pno = pno
        
        if pno != None:
            self.values = self.look()
        else: 
            self.values = []

    flattern = lambda self,a: [item for sublist in a for item in sublist]
    generatex = lambda self,a: np.arange(a)
    
    def look(self):
        """returns list of levels for the time period """
        return self.flattern( df_acc.lookup(self.pno) )
    
    def pnavg(self, delta):
        """returns the average of positive and negetive values"""
        pev = []
        nev = []
        pavg = navg = 0
        
        print type(delta)

        for value in delta:
            if value > 0: 
                pev.append(value)
            elif value < 0:
                nev.append(value)

        if len(pev):
            pavg = sum(pev)/len(pev)
        else:
            pavg = 0

        if len(nev):
            navg = sum(nev)/len(nev)
        else:
            navg = 0

        
        return pavg, navg


    def rateofchange(self,window=7,order=3):
        """Returns two array of the rate of change. (positive and negetive)"""
    
        y = np.array(self.values[1])
        x = self.generatex(len(y))

        #smooting
        yhat = savgol_filter(y, window, 3) 

        #polynomial fitting 
        z = np.polyfit(x, yhat, order)
        f = np.poly1d(z)

        #derivative of polynomial or average rate of change
        delta = f.deriv()
        rate = sum(delta)

        prate , nrate = self.pnavg(delta)

        self.window = window
        self.order = order
        self.x = x
        self.y = y
        self.f = f
        self.yhat = yhat
        self.prate = prate
        self.nrate = nrate

        return prate, nrate

    def plot(self):
        plt.title("Pno: " + self.pno )
        plt.ylabel("Inventory Level")
        plt.xlabel("Days")

        prate = mlines.Line2D(range(1), range(1), color="white", marker="o", \
            markerfacecolor="green",label='+ve rate: '+str(round(self.prate,2)))
        
        nrate = mlines.Line2D(range(1), range(1), color="white", marker="o", \
            markerfacecolor="red",label='-ve rate: '+str(round(self.nrate,2)))
        
        window = mlines.Line2D(range(1), range(1), color="white",  \
            markerfacecolor="red",label='window: '+str(self.window))

        order = mlines.Line2D(range(1), range(1), color="white",  \
            markerfacecolor="red",label='order: '+str(self.order))
        plt.grid(True)


        
        try:
            level, = plt.plot(self.x, self.y, label="levels")
            savgol, = plt.plot(self.x, self.yhat, label="savgol_filter")
            poly, = plt.plot(self.x, self.f(x), label="poly_fit")
            plt.legend(handles=[level,savgol,poly,prate,nrate,window,order])

        except Exception as e:
            print "ERROR: rateofchange() should be called before plot"
            raise e
        


a = Analysis('GM1900126PP')
a.rateofchange()
a.plot()
plt.show()