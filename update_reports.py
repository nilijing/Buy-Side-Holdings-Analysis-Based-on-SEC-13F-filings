#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan  2 22:43:19 2021

@author: yijingtan
"""
mport pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from sec_edgar_downloader import Downloader
import os
import datetime
import re

from sqlalchemy import create_engine

engine = create_engine('mysql+mysqlconnector://nuser:test202012@localhost/sec_reports')

def parse(file, CIK):
    xml = open('/Users/yijingtan/Downloads/d/sec_edgar_filings/' + CIK + '/13F-HR/' + file).read()
    soup = BeautifulSoup(xml, 'lxml')
    try:
        name = soup.find('name').contents[0]
        d1= "".join(soup.find('acceptance-datetime').contents)  
        publish=datetime.datetime.strptime(re.split('\n',d1, maxsplit=0, flags=0)[0],'%Y%m%d%H%M%S') 
        d2=re.split('\n',d1, maxsplit=0, flags=0)[7]
        submit=datetime.datetime.strptime(re.split('\t',d2, maxsplit=0, flags=0)[2], '%Y%m%d')
        quarter=datetime.datetime.strptime(soup.find('reportcalendarorquarter').contents[0],'%m-%d-%Y')
    except:
        return
    cols = ['nameOfIssuer', 'cusip', 'value', 'sshPrnamt','investmentdiscretion']
    attribute= []
    dt=[]
    print("Processing " + name + " (" + CIK + ") for date " + str(publish))

    cos=soup.find_all(re.compile("([A-Za-z0-9]+:|)infotable")) #all companies records in single text
    for co in cos:
      row = []
      for col in cols:
        attribute=co.find([col.lower(), 'n1:' + col.lower()])   #<tag>info</tag>
        #row.append(data.text.strip() if d else 'NaN')
        row.append(attribute.text.strip())
      row.append(submit)
      row.append(publish)
      row.append(quarter)
      row.append(CIK)
      row.append(name)
      dt.append(row)
    df = pd.DataFrame(dt)
    cols.append('submit')
    cols.append('publish')
    cols.append('quarter')
    cols.append('fund_cik')
    cols.append('fund')
    try:
        df.columns = cols
        df.rename(columns={'fund_cik':'CIK','fund':'Fund','publish':'PublishedTime','submit':'SubmittedDate','quarter':'ReportingCalendarQuarter','nameOfIssuer':'NameofIssuer','cusip':'CUSIP','value':'Value(x$1000)','sshPrnamt':'Shares','investmentdiscretion':'InvestmentDiscretion'},inplace=True)
        order =['Fund','CIK','SubmittedDate','PublishedTime','ReportingCalendarQuarter','NameofIssuer','CUSIP','Value(x$1000)','Shares','InvestmentDiscretion']
        df = df[order]
        return df
    except:
        return

def get_ciks(): #input target cik
    ciks = input("Please enter cik separated by comma:  ")     
    c=re.split('[,\ \、\.]',ciks, maxsplit=0, flags=0)
    i=0
    for n in c:
      if n.isdigit():
        i=i+1
        #break
      else:
        print("non-digit, re-input：")
        get_ciks()
    
    return c

def date_range():   
  flag = True
  while flag :
    start=input("Please enter start date(yyyy/m/d):  ")
    try:
      year1,month1,day1=start.split('/')
      flag = False
      start_date=datetime.date(int(year1),int(month1),int(day1))
    except ValueError:
      flag = True
      print ('Date format error') 
  
  date=start_date.strftime('%Y%m%d')
  return date

def get_files(CIK,x):
    save_path = '/Users/yijingtan/Downloads/d' 
    dl = Downloader(save_path)
    dl.get('13F-HR', CIK,include_amends=True,after_date=x) 
    #CIK = CIK.lstrip("0")
    files = os.listdir('/Users/yijingtan/Downloads/d/sec_edgar_filings/' + CIK +'/' +'13F-HR')
    data = [parse(file, CIK) for file in sorted(files)]
    print(data)
    try:
        return pd.concat(data)
    except ValueError:
        print("All Values are None")
        return None


def check_updates(ciks,date_db,dates):
    dd=datetime.datetime.strptime(dates,"%Y%m%d")
    dt=[]
    for i in range(len(c)):
        if date_db[(date_db.cik==ciks[i])].index.tolist():  
            total_interval_time = (dd-date_db.submitteddate[i]).total_seconds() #check date_range with date records
            if total_interval_time >0:
                f=get_files(ciks[i],d) 
                dt.append(f)
            else:
                print('The latest report of '+c[i]+' has been downloaded.')
        else:
            f=get_files(c[i],d)
            dt.append(f)
    target_table=pd.concat(dt, axis=0)
    try:
        return target_table
    except ValueError:
        return None


def save_to_db(df):
        engine = create_engine('mysql+mysqlconnector://nuser:test202012@localhost/sec_reports')
    df.to_sql(name='holdings', con=engine , schema='sec_reports',if_exists='append', chunksize=10000, index=False)
    engine.dispose()





c=get_ciks()    #try with 1478735,1273087
d=date_range()  #try with 20200920

query= '''SELECT cik,submitteddate FROM sec_reports.date_records'''
ciks_db=engine.execute(query).fetchall() 
date_db=pd.DataFrame(ciks_db,columns=['cik','submitteddate'])

df=check_updates(c, date_db, d)
save_to_db(df)     