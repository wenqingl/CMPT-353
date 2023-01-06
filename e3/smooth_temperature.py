import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.nonparametric.smoothers_lowess import lowess
from pykalman import KalmanFilter

# get the file name
f = sys.argv[1]
cpu_data = pd.read_csv(f, parse_dates=[0])

'''
def to_timestamp(x):
    return x.timestamp()
cpu_data['timestamp'] = cpu_data['timestamp'].apply(to_timestamp)'''


# LOESS Smoothing
loess_smoothed = lowess(cpu_data['temperature'], cpu_data['timestamp'], frac=0.035)

# Kalman Smoothing
kalman_data = cpu_data[['temperature', 'cpu_percent', 'sys_load_1', 'fan_rpm']]

initial_state = kalman_data.iloc[0]
observation_covariance = np.diag([1.0, 2.0, 2.0, 1.0]) ** 2 # TODO: shouldn't be zero
transition_covariance = np.diag([0.01, 0.01, 0.01, 0.01]) ** 2 # TODO: shouldn't be zero
transition = [[0.96,0.5,0.2,-0.001], [0.1,0.4,2.3,0], [0,0,0.96,0], [0,0,0,1]] # TODO: shouldn't (all) be zero

kf = KalmanFilter(initial_state_mean=initial_state,
                initial_state_covariance=observation_covariance,
                observation_covariance=observation_covariance,
                transition_covariance=transition_covariance,
                transition_matrices=transition)
kalman_smoothed, _ = kf.smooth(kalman_data)



plt.figure(figsize=(12, 4))
plt.plot(cpu_data['timestamp'], cpu_data['temperature'], 'b.', alpha=0.5)
plt.plot(cpu_data['timestamp'], loess_smoothed[:, 1], 'r-')
plt.plot(cpu_data['timestamp'], kalman_smoothed[:, 0], 'g-')
plt.legend(['Observed Data', 'LOWESS Smoothing', 'Kalman Smoothing'])
plt.xlabel('Timestamp')
plt.ylabel('Temperature')
plt.title('The Temperature of CPU')
#plt.show() # maybe easier for testing
plt.savefig('cpu.svg') # for final submission