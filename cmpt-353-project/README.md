# CMPT353 Final Porject

## Environment Preparation
```
conda env create -f environment.yml
```

## Order of Execution
**clean_accelerate_dataset.ipynb** clean all datasets which collect the accelaration of walking/running\
**clean_GPS_dataset.ipynb** clean the dataset which contains the GPS records (latitude and longitude)\
**ankle_arm_ analysis.ipynb** analysis the position of phone\
**walk-or-run.ipynb** analysis the difference between walks and runs\
**different-people.ipynb** analysis the difference between two people\
**predict_model.ipynb** trains models to make prediction of walking/running

## Files Produced
clean_female_ankle_walk.csv\
clean_male_ankle_run.csv\
clean_male_ankle_walk.csv\
clean_male_arm_walk.csv\
raw_gps.gpx\
smoothed_gps1.gpx\
smoothed_gps2.gpx

