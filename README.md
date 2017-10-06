# Time-Spark

A small python package for time series analysis on Spark. 
![rate-of-change-plot](plt.png)

# You can do the following:
* Load data from multiple CSV files in to Spark RDD
* Create a data structure with selected columns for time series analysis.
* Apply *Savitzky Golay* filtering algorithm for data smoothing. 
* Fit a polynomial function of Nth order to the entire data or its subset.
* Find the derivative and rate of increase or decrease of the data. 

# How is the data stored

Data is stored as `Key, Value` pair 

```
# Eg: 7 days data.

    [(u'HO1230154PP'',

 

      ([105.23, 105.23, 105.23, 105.23, 105.23, 105.23, 105.23],
      [59, 59, 18, 60, 58, 58, 16],
      [u'8/2/2017 9:00:00 PM',
       u'8/3/2017 9:00:00 PM',
       u'8/6/2017 9:00:00 PM',
       u'8/7/2017 9:00:00 PM',
       u'8/8/2017 9:01:00 PM',
       u'8/9/2017 9:00:00 PM',
       u'8/11/2017 9:00:00 PM'])
    ]

```

## Note: This project is in development stage.  
