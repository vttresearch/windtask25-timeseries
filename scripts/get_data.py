# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 08:58:38 2020

@author: ererkka
"""

import os
from multiprocessing.dummy import Pool

import pandas as pd
from entsoe import EntsoePandasClient
from entsoe.mappings import PSRTYPE_MAPPINGS
from entsoe.exceptions import NoMatchingDataError
from pycountry import countries
from dotenv import load_dotenv

load_dotenv()



#%% constants

START = pd.Timestamp("20180101", tz="UTC")
#END = pd.Timestamp("20180107", tz="UTC")
END = pd.Timestamp("20190101", tz="UTC")

COUNTRIES = [
  # Task 25 countries in Europe
 'Denmark',
 'Finland',
 'France',
 'Germany',
 'Ireland',
 'Italy',
 'Netherlands',
 'Norway',
 'Portugal',
 'Spain',
 'Sweden',
 'United Kingdom',
 # Others
 'Belgium',
 'Austria']
#COUNTRIES = ["Finland", "Germany"]

GEN_TYPES = ["Solar", "Wind Onshore", "Wind Offshore"]

#%% functions

# Create client to ENTSO-E TP
client = EntsoePandasClient(api_key=os.getenv("ENTSOE_APIKEY"))

# Invert the psr mapping so that we can get psr types by textual gen. types
inverted_psr_mapping = {value: key for key, value in PSRTYPE_MAPPINGS.items()}


def get_gen_data(country: str, gentype: str) -> pd.Series:
    """Query generation data for country and generation type
    """
    # Get the two-letter country code for this country
    try:
        country_code = countries.get(name=country).alpha_2
    except AttributeError:
        raise KeyError(country)
        
    # Execute the query
    try:
        ts = client.query_generation(
            country_code, start=START, end=END, psr_type=inverted_psr_mapping[gentype]
        )[gentype]  # Select the only column
    except NoMatchingDataError:
        # Create an empty series
        ts = pd.Series(name=(country_code, gentype)).tz_localize('UTC')
    else:
        # Make sure we have real timestamps in UTC
        ts.index = pd.DatetimeIndex(ts.index, name="timestamp").tz_convert("UTC")
        ts.name = (country_code, gentype)
    return ts


def download_parallel(gentype: str, n_threads=1):
    """Download data for a gen. type for different countries in parallel
    """
    with Pool(n_threads) as p:
        series = p.map(lambda c: get_gen_data(c, gentype), COUNTRIES)
    df = pd.concat(series, axis=1).sort_index(0).sort_index(1)
    df.index.name = "timestamp"
    df.columns.names = ["country", "gentype"]
    return df.xs(gentype, axis=1, level=1)


#%% download
for gt in GEN_TYPES:
    df = download_parallel(gt, n_threads=4)
    filename = f"../data/raw/ENTSO-E_TP_generation_{gt}.csv" 
    df.to_csv(filename, header=True)
    print(f"Wrote {filename}")
