# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 16:16:25 2020

@author: ererkka
"""

from pycountry import countries
import pandas as pd
import matplotlib.pyplot as plt
from textwrap import wrap


def get_country_alpha2(country: str) -> str:
    """Get two-letter country code for a country
    """
    # Deal with some special cases first
    if country == "UK":
        cc = "UK"
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


def save_intermediate_data(data: pd.DataFrame, label: str):
    """Save data to intermediate files"""
    data.to_csv(f'../data/intermediate/{label}.csv', header=True)
    

def load_intermediate_data(label: str) -> pd.DataFrame:
    """Load data from intermediate files"""
    df = (pd.read_csv(f'../data/intermediate/{label}.csv',
                      index_col=0, parse_dates=True)
              .dropna(axis=1, how='all'))
    df.index.name = 'Datetime'
    df.columns.name = 'Country code'
    return df


def wrap_xticklabels(ax, length=2):
    """Wrap x-axis tick labels to certain length"""
    ax.set_xticklabels(['\n'.join(wrap(l.get_text(), length)) for l in iter(ax.get_xticklabels())])


def plot_statistics(df: pd.DataFrame, min_timestep=None):
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(20, 4))
    df.plot.box(title='Distribution of values', ax=ax1, showmeans=True)
    if min_timestep is not None:
        df.interpolate('time').diff(1).plot.box(title=f'{min_timestep} changes', ax=ax2)
    df.resample('1H').mean().diff(1).plot.box(title="1 hour changes", ax=ax3)
    ax3.hlines([-0.1, 0.1], *ax3.get_xlim(), color='r')
    ax2.set_ylim(*ax3.get_ylim())
    for ax in (ax1, ax2, ax3): wrap_xticklabels(ax, 3)
    return ax1, ax2, ax3

