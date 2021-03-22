#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct  2 19:43:12 2020

@author: yijingtan
"""

import tkinter as tk
from tkinter.scrolledtext import ScrolledText
import tkinter.messagebox
import pandas as pd
import requests
from bs4 import BeautifulSoup



class search(object):
    def __init__(self,master):
        self.root=master
        self.root.geometry('1500x1500')
        self.root.title('Main window')

        self.var_url=tk.StringVar()
        self.create_page()
        self.event()
       
       
    def create_page(self):
        self.canvas = tk.Canvas(self.root,bg='#2F3032')         
        self.canvas.pack(side=tk.TOP, fill=tk.BOTH, expand=tk.TRUE) 
        self.canvas.pack()
             
        #url input
        tk.Label(self.root,text='URL: ',fg='#A9A9A9',bg='#2F3032').place(x=50,y=100)
        self.var_url=tk.StringVar()
        ent=tk.Entry(self.root,width=50,textvariable=self.var_url,fg='#A9A9A9',bg='#2F3032')
        ent.configure(insertbackground='#A9A9A9')
        ent.place(x=300,y=100)
        
        
        #date range input
        tk.Label(self.root,text='Please enter start date(m/d/yyyy): ',fg='#A9A9A9', bg='#2F3032').place(x=50,y=150)
        tk.Label(self.root,text='Please enter end date(m/d/yyyy):  ',fg='#A9A9A9', bg='#2F3032').place(x=50,y=190) 
        
        self.var_date_0= tk.StringVar()
        #var_date_0.set('example:2/28/2020')   #default       
        ent_0=tk.Entry(self.root,textvariable=self.var_date_0,fg='#A9A9A9',bg='#2F3032',insertbackground='#2F3032')
        ent_0.configure(insertbackground='#A9A9A9')
        ent_0.place(x=300,y=150)
        
        self.var_date_1= tk.StringVar()
        #var_date_1.set('example:12/3/2020')        
        ent_1=tk.Entry(self.root,textvariable=self.var_date_1,fg='#A9A9A9',bg='#2F3032',insertbackground='#2F3032')
        ent_1.configure(insertbackground='#A9A9A9')
        ent_1.place(x=300,y=190)
        
        
        #ScrolledText Box
        self.scr = tk.scrolledtext.ScrolledText(self.root, width=200,height=40,bg='#2F3032',fg='#A9A9A9',wrap =tk.WORD,insertbackground='#2F3032') 
        self.scr.place(x=5,y=280) 
        
        #search button
        self.button1=tk.Button(self.root,text='Search',fg='#A9A9A9',bg='#2F3032')
        self.button1.place(x=450,y=230)

        #download button
        self.b = tk.Button(self.root, text='Download',fg='#A9A9A9',bg='#2F3032',command=self.writeToFile)
        self.b.place(x=520,y=230)
  
    
   
    #search button event  
    def event(self):
        self.button1['command']=self.display

        
    def display(self):
        url=self.var_url.get()
        headers={ 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36'}
        response=requests.get(url=url,headers=headers)
        soup=BeautifulSoup(response.content,'lxml')


        #analyze
        co_nm=soup.find('div', class_='organization-name').text
        items=co_nm.split()
        sep='_'
        names=sep.join(items)
        #co_cik=soup.find('h3').text.split('#')[1]
        
        table = soup.find_all('table')[1]
        
        url_list=[]
        for i in range(1,len(table)-2 ):
            u=table.find_all('a')[i-1]
            url_list.append({
                    'URL':u['href'],
                    'Date':table.find_all('tr')[i].small.text.split(' ')[0]
                    })
    
        col_names=['URL','Date']
        df=pd.DataFrame(columns=col_names,data=url_list) #df is each report's urls list 
        print(df[:5])
        df.to_csv('url_list.csv',encoding='gbk')
        
        #Input date range
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        import datetime
        start_date=self.var_date_0.get()
        end_date=self.var_date_1.get()
        month1,day1,year1=start_date.split('/')
        month1=int(month1)
        day1=int(day1)
        year1=int(year1)
        
        start= datetime.date(year1,month1,day1)
        month2,day2,year2=end_date.split('/')
        month2=int(month2)
        day2=int(day2)
        year2=int(year2)
        end= datetime.date(year2,month2,day2)
        
        d_time = datetime.date(2000,1,1)
        d_time1 =datetime.date.today()
        if end>d_time and start<d_time1:
            pass
        else:
            #show error if input date range exceeds limit 
            tk.messagebox.showerror(title='Error',message='Please enter a correct date.')
        
        
        
        target=df[ (df['Date']<end)&(df['Date']>start) ]
        u=target['URL']
        for i,j in zip( range(len(target)+1),u.index) :
            url=u[j]
            detail_url='https://sec.report'+url
            response=requests.get(url=detail_url,headers=headers)
            soup=BeautifulSoup(response.content,'lxml')
            
        #Get elements
        s=soup.find('div', class_='panel-body context',id='content') 
        Format=s.text.split('Form ')[1]
        Published_time=soup.find('abbr').text
        Submitted=s.text.split('Submitted: ')[1].split('table')[0]
        Period_Ending_In=s.text.split('Period Ending In: ')[1].split('About')[0]
            
        #detail info
        table = soup.find_all('table')[1]
        tds=table.find_all('tr')[1:]
        dff= pd.DataFrame( columns=['CO_Name','Format','Published_time', 'Submitted','Period_Ending_In','Name_of_Issuer','CUSIP','Value(x$1000)','Shares','Investment_Discretion','Voting_Sole/Shared/None'])
        for i in range(len(tds)):
            dff.loc[i]=[names,Format,Published_time,Submitted,Period_Ending_In, sep.join(tds[i].contents[0].text.split()), tds[i].contents[1].text,tds[i].contents[2].text,tds[i].contents[4].text,tds[i].contents[5].text,tds[i].contents[7].text ]


        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns',5000)
        pd.set_option('display.width',10000)
        print('ok')

        #save to text box
        self.scr.insert('insert',dff)

    def writeToFile(self):
        tk.messagebox.showinfo(title='Sub Window',message='Done')
        cur_inp = self.scr.get('1.0', tk.END)
        fl = open('search_result.csv','w')
        fl.write(cur_inp)
        
        print('Done.')
        
     
        
if __name__ =='__main__':
    root=tk.Tk()
    search(root)
    root.mainloop()
     