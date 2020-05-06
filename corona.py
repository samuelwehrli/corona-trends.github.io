import numpy as np
import pandas as pd


# ------------ Helper functions -------------------------  

def apply_to_column(df, column, func, id_vars=['serie','country','state']):
    id_vars = list(set(df.columns) & set(id_vars))
    return df.groupby(id_vars)[column].apply(func) 


# --------------- Classes ------------------------

class DataDownloader:

    @classmethod
    def read_jhu_serie(cls, serie):
        BASE_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_{}_global.csv'
        ID_DICT = {'Country/Region':'country','Province/State':'state'}
        return ( 
            pd.read_csv(BASE_URL.format(serie)) 
            .fillna('')
            .drop(columns=['Lat','Long'])
            .rename(columns=ID_DICT) 
            .melt(id_vars=['country','state'],var_name='date',value_name='cumulative')
            .query('cumulative > 0')
            .reset_index(drop=True) 
            .assign(date = lambda df: df.date.apply(pd.to_datetime))
            .assign(serie=serie))[['date','serie','country','state','cumulative']]
    
    @classmethod
    def read_jhu(cls, series = ['confirmed','deaths']):
        df = [cls.read_jhu_serie(serie) for serie in series]
        df = pd.concat(df).reset_index(drop=True)
        df['daily'] = apply_to_column(df,'cumulative',lambda s: s.diff().fillna(s.values[0]).astype(int))
        return df
    
