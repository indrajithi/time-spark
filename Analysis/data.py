import numpy as np
import pandas as pd
import pylab as pl
import csv
import glob
import os
from pyspark.sql import SQLContext
from pyspark.sql.types import *


class Data:
    """imports data to spark RDD"""
    def __init__(self,datadir):





class Frame:
    """Class for rate of change, ploting and other analysis
    Attributes: 
                values: list of inventory levels for the time period 
    """
    def __init__(self, pno):
        self.pno = pno
        self.values = self.look()

    flattern = lambda self,a: [item for sublist in a for item in sublist]
    generatex = lambda self,a: np.array(self.values)
    
    def look(self):
        """returns list of levels for the time period """
        return self.flattern( df_acc.lookup(self.pno) )

    def rateofchange(self,window=5):
        """Returns two array of the rate of change. (positive and negetive)"""
    
        y = self.values
        x = self.generatex(y)
        print(x)
        print(y)
