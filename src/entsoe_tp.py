# -*- coding: utf-8 -*-
"""
Created on Thu Apr 16 09:14:32 2020

@author: ererkka
"""
from typing import Callable
from multiprocessing.dummy import Pool

from requests.exceptions import HTTPError
from entsoe import EntsoePandasClient
from entsoe.mappings import PSRTYPE_MAPPINGS, TIMEZONE_MAPPINGS
from entsoe.exceptions import NoMatchingDataError
import pandas as pd

from .const import TZ, START, END


# Invert the psr mapping so that we can get psr types by textual gen. types
inverted_psr_mapping = {value: key for key, value in PSRTYPE_MAPPINGS.items()}


def download_parallel(function: Callable, arguments, 
                      n_threads=1,
                      index_name=None, 
                      columns_name=None) -> pd.DataFrame:
    """Download data using a defined function
    
    Args:
        function: The query function to use, must return a pandas Series
        arguments: List of tuples to pass as arguments to `function`
        n_threads: Number of threads to use
        index_name (optional): Name for index
        columns_name (optional): Name(s) for the columns
    """
    try:
        with Pool(n_threads) as p:
            series = p.starmap(function, arguments)
    except ConnectionError:
        raise
    except HTTPError as e:
        print(e.response)
        raise e

    df = pd.concat(series, axis=1)
    if index_name is not None:
        df.index.name = index_name
    if columns_name is not None:
        df.columns.names = columns_name
    return df.sort_index(0).sort_index(1)


def harmonize_datetime_index(ts: pd.Series) -> pd.Series:
    """Aux. function to make sure we have real timestamps in UTC"""
    return (pd.Series(ts.values, 
                      index=pd.DatetimeIndex(ts.index, name="timestamp")
              .tz_convert(TZ)))


class ENTSO_E_TP_Downloader(object):
    
    def __init__(self, apikey):
        self.client = EntsoePandasClient(api_key=apikey)

    def get_gen_data(self, domain: str, gentype: str) -> pd.Series:
        """Query generation data for domain and generation type
        """ 
        # Execute the query
        ts = pd.Series(name=(domain, gentype)).tz_localize(TZ)
        try:
            df = self.client.query_generation(
                domain, 
                start=pd.Timestamp(START, tz=TZ), 
                end=pd.Timestamp(END, tz=TZ), 
                psr_type=inverted_psr_mapping[gentype],
            )
        except NoMatchingDataError:
            print(f"No data for {gentype} in {domain}!")
        except ValueError:
            print(f"Error getting data for {gentype} in {domain}!")
        else:
            # Make sure we have real timestamps in UTC
            ts = harmonize_datetime_index(df[gentype])
            ts.name = (domain, gentype)
        return ts.sort_index()
    
    def get_installed_cap_data(self, gentype: str, 
                               domain: str) -> pd.Series:
        """Query generation data for domain and generation type
        """
        # Time series name has the arguments
        ts_name = (gentype, domain)
        ts = pd.Series(name=ts_name)    
        # Execute the query
        try:
            df = self.client.query_installed_generation_capacity(
                domain, 
                start=pd.Timestamp(f'{pd.Timestamp(START).year}-01-01',
                                   tz=TIMEZONE_MAPPINGS[domain]), 
                end=pd.Timestamp(f'{pd.Timestamp(END).year}-12-31', 
                                 tz=TIMEZONE_MAPPINGS[domain]),
                psr_type=inverted_psr_mapping[gentype]
            )
        except NoMatchingDataError:
            print(f"No data for {gentype} in {domain}!")
        else:
            ts = df[gentype]  # Select the only column
            ts.index = [pd.Timestamp(year=t.year, month=1, day=1) 
                        for t in ts.index]  # ENTSO-E has numbers for the beginning of the year
            ts.index.name = 'Date'
            ts.name = ts_name
        return ts
    
    def get_load_data(self, domain: str) -> pd.Series:
        """Query generation data for country and generation type
        """       
        # Execute the query
        ts = pd.Series(name=domain).tz_localize(TZ)
        try:
            ts = self.client.query_load(
                domain, 
                start=pd.Timestamp(START, tz=TZ), 
                end=pd.Timestamp(END, tz=TZ)
                )
        except NoMatchingDataError:
            print(f"No data for {domain}!")
        except ValueError:
            print(f"Error getting data for {domain}!")
        else:
            ts = harmonize_datetime_index(ts)
            ts.name = domain
        return ts.sort_index()
    
    def get_fcast_data(self, domain: str, gentype: str) -> pd.Series:
        """Query generation data for country and generation type
        """       
        # Execute the query
        ts = pd.Series(name=(domain, gentype)).tz_localize(TZ)
        try:
            df = self.client.query_wind_and_solar_forecast(
                domain, 
                start=pd.Timestamp(START, tz=TZ), 
                end=pd.Timestamp(END, tz=TZ), 
                psr_type=inverted_psr_mapping[gentype]
            )
        except NoMatchingDataError:
            print(f"No data for {gentype} in {domain}!")
        except ValueError:
            print(f"Error getting forecast data for {gentype} in {domain}!")
        else:
            ts = harmonize_datetime_index(df[gentype])
            ts.name = (domain, gentype)
        return ts.sort_index()
