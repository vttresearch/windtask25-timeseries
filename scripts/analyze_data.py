# -*- coding: utf-8 -*-
"""
Created on Fri Jan 17 09:01:25 2020

@author: ererkka
"""

import pandas as pd

from tabulate import tabulate

#%%

def pretty_print(table: pd.DataFrame) -> str:
    return tabulate(table, headers='keys',
                    floatfmt='.2f', numalign='right')

for tech in ['Onshore', 'Offshore']:
    df = pd.read_csv(f'../data/processed/Wind CF {tech}.csv',
                     index_col=0, parse_dates=True)
    print(tech)
    print('----------------------------------------')
    print('Time series statistics')
    print(pretty_print(df.describe()))
    print('----------------------------------------')
    print("Diff(1) statistics")
    print(pretty_print(pd.concat([df[c].dropna().diff(1).describe()
                                  for c in df.columns], axis=1)))
    print('========================================')

