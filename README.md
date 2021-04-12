IEA Wind, Task 25 time series database
======================================

This repository contains Jupyter Notebooks and Python source code used
to create the Task 25 time series database. Please note that neither the 
orginal nor processed data files are included here.

## Data cleaning

Missing values in the raw time series were interpolated up to four consecutive time steps.

An algortihm to remove sudden peaks and drops from original data was developed. 
See functions `remove_drops()` and `remove_peaks()` in module [`src.data_cleaning`](src/data_cleaning.py).
The main functionality is based on [`scipy.signal.find_peaks`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.find_peaks.html).
The idea was to detect single time step peaks (or drops) or ‘plateaus’ of two time steps.
The threshold value for a peak was set to the smallest absolute change where the change
was greater than one standard deviation away from the mean change. 
Plateau prominence was set to the same threshold.
Peaking (dropping) values were removed and the gaps interpolated from the edges.
Highest mean-normalised RMSE of the filtered time series for onshore wind generation was 
less than 10% (Great Britain), see more analysis of the data cleaning process in the Notebook 
[*02b-analyze-cleaning.ipynb*](notebooks/02b-analyze-cleaning.ipynb).

Some even larger errors were removed manually. For example, GB data had a 6-hour sudded drop of
relative load from above 0.7 to under 0.1. This period was cleared and interpolated linearyl from the edges.
See notebook [04-analyze-data.ipynb](notebooks/04-analyze-data.ipynb) for details.

