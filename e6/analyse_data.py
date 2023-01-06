import pandas as pd
from scipy import stats
from statsmodels.stats.multicomp import pairwise_tukeyhsd
import matplotlib.pyplot as plt

data = pd.read_csv('data.csv')
p = stats.f_oneway(data['qs1'], data['qs2'], data['qs3'], data['qs4'], data['qs5'], data['merge1'], data['partition_sort']).pvalue
#print(p)
melt = pd.melt(data)

posthoc = pairwise_tukeyhsd(
    melt['value'], melt['variable'],
    alpha=0.05)

fig = posthoc.plot_simultaneous()

#plt.show()
print(posthoc)