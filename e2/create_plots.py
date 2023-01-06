import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# get the file name
filename1 = sys.argv[1]
filename2 = sys.argv[2]

# get the data of the file
file1 = pd.read_csv(filename1, sep=' ', header=None, index_col=1,
        names=['lang', 'page', 'views', 'bytes'])

file2 = pd.read_csv(filename2, sep=' ', header=None, index_col=1,
        names=['lang', 'page', 'views', 'bytes'])

# sort the first data set by the number of views (decreasing)
file1_sorted = file1.sort_values(by='views', ascending=False)

# plot 1
plt.figure(figsize=(10, 5)) # change the size to something sensible
plt.subplot(1, 2, 1) # subplots in 1 row, 2 columns, select the first
plt.plot(file1_sorted['views'].values) # build plot 1
plt.title('Distribution of Views')
plt.xlabel('Rank')
plt.ylabel('Views')


# a scatterplot of views from the first data file (x-coordinate) 
# and the corresponding values from the second data file (y-coordinate).
file1['views2'] = file2['views']


# plot 2
plt.subplot(1, 2, 2) # ... and then select the second
plt.plot(file1['views'], file1['views2'], 'b.') # build plot 2
plt.xscale('log')
plt.yscale('log')
plt.title('Hourly Views')
plt.xlabel('Views from the first data file')
plt.ylabel('Views from the second data file')

plt.savefig('wikipedia.png')

