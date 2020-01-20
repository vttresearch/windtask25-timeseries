# -*- coding: utf-8 -*-
"""
Created on Fri Jan 17 10:17:03 2020

@author: ererkka
"""

from datetime import date
from glob import glob

import pandas as pd
from pycountry import countries

#%%
frames = list()
for filepath in glob("../data/external/Cumulative_capacity_table_data_*.csv"):
    frames.append(pd.read_csv(filepath))
capacity_data = pd.concat(frames)

#%%
def get_country_alpha2(country: str) -> str:
    """Get two-letter country code for a country
    """
    # Deal with some special cases first
    if country == "UK":
        cc = "GB"
    elif country == "FYROM":
        cc = "MK"
    elif country == 'Kosovo':
        cc = 'XK'
    else:
        try:
            cc = countries.get(name=country).alpha_2
        except AttributeError:
            try:
                cc = countries.search_fuzzy(country)[0].alpha_2
            except LookupError:
                cc = country
    return cc

# Set country codes
capacity_data["Country code"] = capacity_data["Country"].apply(get_country_alpha2)

# Set dates to beginning of the year
capacity_data["Date"] = capacity_data["Year"].apply(
    lambda y: date(y + 1, 1, 1)
)

#%%
# Create daily index for the whole period
idx = pd.date_range(capacity_data['Date'].min(), 
                    capacity_data['Date'].max(),
                    freq='D')

df = capacity_data[['Country code', 'Date']].copy()
df['Onshore'] = capacity_data["Cumulative onshore capacity"]
df['Offshore'] = capacity_data["Cumulative offshore capacity"]
wind_capacity = (df.set_index(["Country code", "Date"])
                   .unstack(0).reindex(idx).interpolate())


#%%

for tech in ['Onshore', 'Offshore']:
    wind_generation = pd.read_csv(f"../data/raw/ENTSO-E_TP_generation_Wind {tech}.csv", 
                                  index_col=0, parse_dates=True)
    wind_cf = pd.DataFrame(index=wind_generation.index)
    for cc in wind_generation.columns:
        gen = wind_generation[cc].dropna()
        cap = pd.Series(wind_capacity[(tech, cc)], name='cap')
        df = pd.DataFrame({'gen': gen,
                           'date': pd.DatetimeIndex(gen.index.date)
                           }).join(cap, on='date')
        wind_cf[cc] = df['gen'] / df['cap']
    wind_cf.to_csv(f'../data/processed/Wind CF {tech}.csv', header=True)
        


