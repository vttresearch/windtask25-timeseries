# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 15:39:57 2020

@author: ererkka
"""

import pandas as pd
import numpy as np
from scipy.signal import find_peaks
from scipy.stats import zscore

MAX_GAP_TO_FILL = 4
FILTER_WIDTH = 3
Z_SCORE_THRESHOLD = 1


def expand_to_full_length(ts: pd.Series, stats: pd.DataFrame, 
                          start: str, end: str,
                          tzone: str = None,
                          max_gap: int = MAX_GAP_TO_FILL,
                          min_timestep: str = None) -> pd.Series:
    """Reindex a time series to the full length and fill in gaps 
    
    Args:
        ts: Time series to process
        stats: Data frame for cleaning statistics
        start, end: Start and end times
        tzone (optional): Desired output time zone
        max_gap: Maximum length of gap to fill (time steps) or `None` for no limit
        min_timestep: Minimum time step length used (see Pandas Timedelta)
    """
    ts_ = ts.dropna()  # Drop missing values
    if not len(ts_):
        return ts  # Return original in case of no data after droppping missing values
    # Get time series frequency
    if not ts_.index.freq:
        freq = ts_.index[1] - ts_.index[0]  # Infer the frequency
    else:
        freq = ts_.index.freq
    # Check we got something reasonable
    if min_timestep is not None and freq < pd.Timedelta(min_timestep):
        raise RuntimeError(f"Series has frequency {freq} which is "
                           "below the given minimum")
    # Make the full time index and reindex uisng original time zone
    time_idx = pd.date_range(start, end, tz=tzone,
                             freq=freq, closed='left')
    reindexed = ts_.tz_convert(tzone).reindex(time_idx)
    stats.loc[ts.name, 'expanded length'] = len(reindexed)
    # interpolate if necessary
    if reindexed.isna().sum():
        print(f"Filling gaps for {ts.name}. . .")
        stats.loc[ts.name, 'original coverage'] = 1 - reindexed.isna().sum() / len(reindexed)
        interpolated = reindexed.interpolate(method='time', limit_area='inside', 
                                             limit=max_gap
                                            )
        stats.loc[ts.name, 'interpolated coverage'] = 1 - interpolated.isna().sum() / len(interpolated)
        stats.loc[ts.name, 'missing values'] = interpolated.isna().sum()
        return interpolated
    else:
        stats.loc[ts.name, 'original coverage'] = 1.0
        stats.loc[ts.name, 'missing values'] = 0
        return reindexed
    
    
def rmse_of_filter(original: pd.Series, filtered: pd.Series):
    """Calculate RMSE between original and filtered time series"""
    return (original - filtered).pow(2).mean() ** 0.5


def median_filter(ts: pd.Series, stats: pd.DataFrame = None, 
                  second_pass=False):
    """Apply rolling median filter to time series"""
    _ts = ts.dropna()  # Make sure there are no empty values
    filtered = _ts.copy()

    # Assing rolling median from 2nd to 2nd last index
    filtered.iloc[1:-1] = filtered.rolling(FILTER_WIDTH, center=True).median()

    # Second pass
    if second_pass:
        difference = filtered.diff()
        QUANTILE = 0.01
        big_changes = difference[(difference < difference.quantile(QUANTILE)) | 
                                 (difference > difference.quantile(1 - QUANTILE))]
        filtered.loc[big_changes.index] = (
            filtered.rolling(2 * FILTER_WIDTH, center=True).median()
        )

    # Calculate RMSE of filter
    rmse = rmse_of_filter(_ts, filtered)
    
    if stats is not None:
        stats.loc[ts.name, 'RMSE of filter'] = rmse
        return filtered
    else:
        return filtered, {'RMSE of filter': rmse}


def sudded_change_filter(ts):
    """Clear sudden changes from a time series"""
    _ts = ts.dropna()
    filtered = _ts.copy()
    difference = _ts.diff().reset_index(drop=True)
    quantile = 0.01
    big_changes = difference[(difference < difference.quantile(quantile)) | 
                             (difference > difference.quantile(1 - quantile))]
    
    def find_rebound(i: int):
        for j in range(i, len(difference)):
            if (abs(difference.loc[i] - -difference.loc[j]) / abs(difference.loc[i])) < 0.5:
                return j
        return None

    j = -1
    for i in big_changes.index:
        if i <= j:
            continue
        j = find_rebound(i)
        if j is not None:
            filtered.iloc[i:j] = np.nan
        else:
            j = i + 1

    ts.plot()
    filtered.interpolate().plot()
    ts.iloc[big_changes.index].plot(style='*')

    return filtered.interpolate()


def remove_drops(original, z_score_threshold=Z_SCORE_THRESHOLD, two_pass=False, stats_df=None):
    "Remove sudden drops from a time series"
    
    ts = original.dropna()  # Make sure there are no empty values
    ts_diff = ts.diff()                                                   
    diff_z_scores = pd.Series(zscore(ts_diff, nan_policy='omit'), 
                              index=ts_diff.index)

    peak_threshold = -(ts_diff[diff_z_scores < -Z_SCORE_THRESHOLD].max())
    
    # Find peaks
    peaks, _ = find_peaks(-ts, threshold=peak_threshold)
    
    # Find plateaus
    _, plateau_properties = find_peaks(-ts, prominence=peak_threshold,
                                       plateau_size=2,
                                       width=(None, 4), 
                                       rel_height=1)
    plateaus = np.concatenate((plateau_properties['left_edges'],
                               plateau_properties['right_edges']))
    
    all_peaks = np.concatenate((peaks, plateaus))
    all_peaks.sort()
    
    cleaned = ts.copy()
    cleaned.iloc[all_peaks] = np.nan
    cleaned.interpolate('time', inplace=True)
    
    if two_pass:
        peaks2, _ = find_peaks(-cleaned, threshold=peak_threshold)
        cleaned.iloc[peaks2] = np.nan
        cleaned.interpolate('time', inplace=True)
        
    # Calculate RMSE of filter
    rmse = rmse_of_filter(ts, cleaned)
    
    if stats_df is not None:
        stats_df.loc[ts.name, 'RMSE of filter'] = rmse
        return cleaned
    else:
        return cleaned, {'RMSE of filter': rmse}
    

def remove_peaks(original, z_score_threshold=Z_SCORE_THRESHOLD, two_pass=False, stats_df=None):
    cleaned, statistics = remove_drops(-original, z_score_threshold=z_score_threshold, 
                                  two_pass=two_pass, stats_df=None)
    if stats_df is not None:
        for key, value in statistics.items(): 
            stats_df.loc[original.name, key] = value
        return -cleaned
    else:
        return -cleaned, {'RMSE of filter': rmse}
