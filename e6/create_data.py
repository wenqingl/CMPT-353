import time
import pandas as pd
import numpy as np
from implementations import all_implementations

times = 90
size = 9000

data = pd.DataFrame(index = np.arange(times),
                    columns = ['qs1', 'qs2', 'qs3', 'qs4', 'qs5', 'merge1', 'partition_sort'])

for i in range(times):
    random_array = np.random.randint(-100,100,size)
    for sort in all_implementations:
        st = time.time()
        res = sort(random_array)
        en = time.time()
        data.at[i, sort.__name__] = en-st
        #print(sort.__name__)
        
#print(data)
data.to_csv('data.csv', index=False)
