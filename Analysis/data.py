import numpy as np
import pandas as pd
import pylab as pl
import csv
import glob
import os
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.types import sqlContext

def importdata(datadir, filelds = ['PartNumber','QuantityAvailable','DateCreated','CustomerPrice']):
    """imports data to spark RDD"""
    file_path   = os.path.join(".",datadir)
    files       = glob.glob(file_path + "/*")
    files.sort()

    cwd = os.getcwd()
    


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

    def findsale(a):
        sale , repl = Sale(a), Replenish(a)
        if (a[0] + repl ) != 0:
            percentage_sale = (float(sale) / (a[0] + repl )) *100 
        else: percentage_sale = 0

        return sale, repl, round(percentage_sale, 2)

    first_flag = True
    for file_no in range(len(files)):
        
        #load csv in pandas and create dataframe
        f1 = pd.read_csv(files[ file_no ], sep="|", usecols=filelds)
        df =  sqlContext.createDataFrame( f1 )
        
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

    return df_acc

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
        
        n = len(self.values[1])

        for i in range(n):
            value = delta(i)

            if value > 0: 
                pev.append(value)
            elif value < 0:
                nev.append(value)

        if len(pev):
            pavg = sum(pev)/float(n)
        else:
            pavg = 0

        if len(nev):
            navg = sum(nev)/float(n)
        else:
            navg = 0

        
        return pavg, navg

    def avgrot(self,delta):
        pev = []
        nev = []
        pavg = navg = 0

        for i in range(len(self.values)):
            value = delta(i)

    def rateofchange(self,window=7,order=3,savgol_order=3):
        """Returns two array of the rate of change. (positive and negetive)"""
    
        y = np.array(self.values[1])
        x = self.generatex(len(y))

        #smooting
        yhat = savgol_filter(y, window, savgol_order) 

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
        
        plt.ylabel("Inventory Level")
        plt.xlabel("Days")

        pno = mlines.Line2D(range(1), range(1), color="white",  \
            markerfacecolor="red",label='PNo: '+str(self.pno))

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
            poly, = plt.plot(self.x, self.f(self.x), label="poly_fit")
            plt.legend(handles=[pno,level,savgol,poly,prate,nrate,window,order])

        except Exception as e:
            print "ERROR: rateofchange() should be called before plot"
            raise e

def Multyplot(pnos):
    count = 0
    for pno in pnos:
        a = Analysis(pno)
        a.rateofchange()

        if len(pnos) >=4 and count % 4 == 0:
            fig, ax = plt.subplots(nrows=2,ncols=2)
        if len(pnos) >= 4:
            plt.subplot(2,2, count %4 + 1 )
        if len(pnos) == 2:
            plt.subplot(2,1, count %2 + 1)
        if len(pnos) == 3:
            plt.subplot(2,2, count %4 + 1 )
    
        a.plot()
        count +=1
    plt.show()


