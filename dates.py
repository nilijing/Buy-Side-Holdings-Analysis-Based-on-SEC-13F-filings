#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  2 07:07:03 2021

@author: yijingtan
"""

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql+mysqlconnector://nuser:test202012@localhost/sec_reports')

query= '''SELECT DISTINCT CIK,SubmittedDate FROM sec_reports.holdings'''
re=engine.execute(query).fetchall()   

''' list example:
('1393818', datetime.datetime(2020, 11, 16, 0, 0))
('1478735', datetime.datetime(2020, 9, 15, 0, 0))
('1478735', datetime.datetime(2020, 11, 16, 0, 0))'''

d=pd.DataFrame(re,columns=['cik','submitteddate'])

#delete duplicate ciks and older dates
def record_submit_date(d):
    dates1=d.copy()
    dates2=d.copy()
    for i in range(len(dates1)-1):
        if dates1.iloc[i,0]==dates1.iloc[i+1,0]:
            dates2=dates2.drop(i,axis=0)
        else:
            print(dates1.iloc[i,0]+' for '+str(dates1.iloc[i,1])+' has done.')
    return dates2
        
#save to database
def save_to_db(df):
    engine = create_engine('mysql+mysqlconnector://nuser:test202012@localhost/sec_reports')
    df.to_sql(name='date_records', con=engine , schema='sec_reports',if_exists='replace', chunksize=10000, index=False)
    engine.dispose()


dates=record_submit_date(d)
save_to_db(dates)