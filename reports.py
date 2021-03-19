#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 29 01:56:14 2020

@author: yijingtan
"""
import pandas as pd
from bs4 import BeautifulSoup
import os
import datetime
import re

#!pip install -U sec-edgar-downloader
from sec_edgar_downloader import Downloader

#!pip3 install pymysql
from sqlalchemy import create_engine



"""#1.
Funtion parse(file, CIK) to parse downloaded txt files.
"""

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

    cos=soup.find_all(['ns1:infotable', 'infotable']) #all companies records in single text
    for co in cos:
      row = []
      for col in cols:
        attribute=co.find([col.lower(), 'ns1:' + col.lower()])   #<tag>info</tag>
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

"""#2.
Filter by date
"""

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

"""Function get_files() to download target files."""

def get_files(CIK,x):
    save_path = '/Users/yijingtan/Downloads/d'  
    dl = Downloader(save_path)
    dl.get('13F-HR', CIK,include_amends=True,after_date=x) 
    CIK = CIK.lstrip("0")
    files = os.listdir('/Users/yijingtan/Downloads/d/sec_edgar_filings/' + CIK +'/' +'13F-HR')

    data = [parse(file, CIK) for file in sorted(files)]

    try:
        return pd.concat(data)
    except ValueError:
        print("All Values are None")
        return None

"""#3.
Function get_ciks() to obtain target compaines' ciks.
"""

def get_ciks():#input target cik
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

#Take two funds: Blackstone Group Inc and TWO SIGMA ADVISERS, LP and their CIKs as examples (0001393818,0001478735).
ciks=get_ciks() 

"""#4.
Final dataframe of search results.
"""
i = 1
#Get files from Edgar
dt=[]
x=date_range()
for cik in ciks:  
    print("----- " + str(i) + " of " + str(len(ciks)) + " Funds -----")
    i+= 1

    # Download files
    d=get_files(cik,x) #DataFrame
    dt.append(d)

target_table=pd.concat(dt, axis=0)
target_table


"""#5.
Save search results into database
"""
def save_to_db(df):
    engine = create_engine('mysql+mysqlconnector://nuser:test202012@localhost/sec_reports')
    df.to_sql(name='holdings', con=engine , schema='sec_reports',if_exists='append', chunksize=10000, index=False)
    engine.dispose()

save_to_db(target_table)


