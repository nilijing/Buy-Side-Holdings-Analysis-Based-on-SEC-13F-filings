#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan  3 22:20:20 2021

@author: yijingtan
"""
import pandas as pd
from sqlalchemy import create_engine
import urllib.request


def get_record(url):
    resp = urllib.request.urlopen(url)
    ele_json = json.loads(resp.read())
    return ele_json

if __name__ == '__main__':
    f=get_record('https://www.sec.gov/files/company_tickers.json')


    
print(type(f))  #dict
df =pd.DataFrame.from_dict(f, orient='index')


def save_to_db(df):
    engine = create_engine('mysql+mysqlconnector://nuser:test202012@localhost/sec_reports')
    df.to_sql(name='ciks', con=engine , schema='sec_reports',if_exists='replace', chunksize=1000, index=False)
    engine.dispose()

save_to_db(df)