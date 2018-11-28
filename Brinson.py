
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
#import pyodbc
import datetime
import datetime
import dateutil

from dateutil.parser import parse
import os,pymssql
import re
import math

def get_data(sql1):
    server="192.168.200.26"
    user="jytest"
    password="jy0608"
    conn=pymssql.connect(server,user,password,database="JYDB",charset='utf8')
    cursor=conn.cursor()
    cursor.execute(sql1)
    row=cursor.fetchall()
    conn.close()
    data =pd.DataFrame(row,columns=zip(*cursor.description)[0])
    data = l2gbyR(data)
    return data

def latin2gbk(s):
    if type(s)==unicode:
        s = s.encode('latin1').decode('gbk')
    elif s is None:
        s = np.nan
    return s

def l2gbyR(data):
    for i in data.columns:    
        try:
            data[i] = data[i].apply(lambda s: latin2gbk(s))
        except:
            continue
    return data

def re_s(s,strr):
    pattern = re.compile(strr)
    m = pattern.search(s)
    if m is not None:
        return m.group()
    else:
        return np.nan
    
def decimal_to_float(df,s):
    for i in s:
        df[i] = df[i].apply(lambda s:float(s))
    return df

def get_last_season_enddate(latest_date):
    #获取上一个season的endDate
    #input必须为pandas的datetime变量
    year=latest_date.year
    month=latest_date.month
    
    if month in [3,6]:
        day='31'
    else:
        day='30'
        
    if month==3:
        year=str(year-1)
        month=str(12)
    else:
        year=str(year)
        month='0'+str(month-3)
        
    last_season_end_date="'"+year+'-'+month+'-'+day+"'"
    
    return last_season_end_date


# # 基准数据处理

# In[4]:


hs300data=get_data("select b.SecuCode as IndexCode,b.SecuAbbr as IndexName,c.SecuCode,c.SecuAbbr,a.Weight from LC_IndexComponentsWeight a,SecuMain b, SecuMain c  where a.IndexCode=b.InnerCode and b.SecuCode='000300'and a.UpdateTime>'2018-09-28' and a.UpdateTime<'2018-10-28'and a.InnerCode=c.InnerCode")


# In[5]:


hs300data.to_excel('hs300data.xlsx')


# In[6]:


hs300data=pd.read_excel('hs300data.xlsx')


# In[7]:


hs300data['BenRtn_Prod']=hs300data['Rtn']*hs300data['Weight']
ben_hs300=pd.DataFrame()
ben_hs300=hs300data[['Industry','BenRtn_Prod','Weight']].groupby(['Industry']).sum().reset_index()
ben_hs300['BenRtn']=ben_hs300['BenRtn_Prod']/ben_hs300['Weight']
ben_hs300=ben_hs300.dropna().reset_index(drop=True)
del ben_hs300['BenRtn_Prod']


# In[9]:


ben_hs300_0930=ben_hs300


# In[11]:


ben_hs300_0930.to_excel('ben_hs300_0930.xlsx')


# In[12]:


zh800data=pd.read_excel('zh800data.xlsx')
zh800data['BenRtn_Prod']=zh800data['Rtn']*zh800data['Weight']
ben_zh800=pd.DataFrame()
ben_zh800=zh800data[['Industry','BenRtn_Prod','Weight']].groupby(['Industry']).sum().reset_index()
ben_zh800['BenRtn']=ben_zh800['BenRtn_Prod']/ben_zh800['Weight']
ben_zh800=ben_zh800.dropna().reset_index(drop=True)
del ben_zh800['BenRtn_Prod']
ben_zh800_0930=ben_zh800


# In[13]:


ben_zh800_0930.to_excel('ben_zh800_0930.xlsx')


# # Main Function

# In[35]:


fund_code=pd.read_excel(u'Q3List.xlsx',dtype={'FundCode':str})
fund_code_list=fund_code['FundCode'].tolist()

ben=pd.read_excel('ben_zh800_0930.xlsx')
fund_code_list=fund_code_list[0:30]


# In[36]:


funddata_all=list() #储存最后结果
for k in range(len(fund_code_list)):
    hld_data1=pd.DataFrame()

    
    fundcode="'"+str(fund_code_list[k])+"'"
    
    hld_data=get_data("SELECT b.SecuCode as FundCode,b.SecuAbbr as FundName,c.SecuCode,c.SecuAbbr,a.SharesHolding,a.MarketValue,a.ReportDate                      FROM MF_KeyStockPortfolio a,SecuMain b,SecuMain c                       WHERE a.InnerCode=b.InnerCode and b.SecuCode="+ fundcode + " and a.StockInnerCode=c.InnerCode and                       a.ReportDate='2018-09-30'and b.SecuCategory=8")
    
    
    #读取基金持仓行业名称


    codes = [str(s) for s in hld_data['SecuCode']]
    
    hld_industryname=get_data("SELECT b.SecuCode,a.FstIndNameCSRS from LC_CSIIndustry a, SecuMain b                                where a.InnerCode=b.InnerCode and b.SecuCode in {} and                                a.EndDate=(select max(c.EndDate) from LC_CSIIndustry c where a.InnerCode=c.InnerCode)".format(tuple(codes)))

    #将基金基础数据与行业数据merge为hld_data1
    hld_data1=pd.merge(hld_data,hld_industryname)
    
    
    
    
    #读取并计算头尾持仓股价收益率
    b=pd.DataFrame()
    hld_price=pd.DataFrame()

    hld_data1['ReportDate']=pd.to_datetime(hld_data1['ReportDate'])
    current_season_date=hld_data1.iloc[0].loc['ReportDate']
    last_season_date=get_last_season_enddate(current_season_date)
    current_season_date="'"+str(current_season_date)+"'"

    

    for code in range(len(hld_data['SecuCode'])):
        
        i="'"+str(hld_data['SecuCode'][code])+"'"
    
        try:
            
        
            yuju="SELECT c.SecuCode, a.PrevClosePrice as BgnPeriodPrice,            b.ClosePrice as EndPeriodPrice,LOG(b.ClosePrice/a.PrevClosePrice) as LogReturn             FROM QT_DailyQuote a,QT_DailyQuote b,SecuMain c,SecuMain d             WHERE c.SecuCategory=1 and d.SecuCategory=1 AND a.InnerCode=c.InnerCode             AND b.InnerCode=d.InnerCode AND c.SecuCode="+ i +" AND d.SecuCode="+ i +"             AND b.TradingDay=(select Max(e.TradingDay) FROM QT_DailyQuote e,SecuMain i             WHERE e.InnerCode=i.InnerCode AND i.SecuCategory=1 AND i.SecuCode="+ i +"             AND e.TradingDay <="+ current_season_date +") AND             a.TradingDay=(select MIN(h.TradingDay) FROM QT_DailyQuote h,SecuMain j             WHERE h.InnerCode=j.InnerCode AND j.SecuCategory=1 AND j.SecuCode="+ i +"             AND h.TradingDay > "+last_season_date+")"
        
            b=get_data(yuju)
    
        except:
            
            
        
            yuju="SELECT c.SecuCode, a.ClosePrice as BgnPeriodPrice,            b.ClosePrice as EndPeriodPrice, LOG(b.ClosePrice/a.ClosePrice) as LogReturn             FROM QT_DailyQuote a,QT_DailyQuote b,SecuMain c,SecuMain d             WHERE c.SecuCategory=1 and d.SecuCategory=1 AND a.InnerCode=c.InnerCode             AND b.InnerCode=d.InnerCode AND c.SecuCode="+ i +" AND d.SecuCode="+ i +"             AND b.TradingDay=(select Max(e.TradingDay) FROM QT_DailyQuote e,SecuMain i             WHERE e.InnerCode=i.InnerCode AND i.SecuCategory=1 AND i.SecuCode="+ i +"             AND e.TradingDay <="+ current_season_date +") AND             a.TradingDay=(select MIN(h.TradingDay) FROM QT_DailyQuote h,SecuMain j             WHERE h.InnerCode=j.InnerCode AND j.SecuCategory=1 AND j.SecuCode="+ i +"             AND h.TradingDay > "+last_season_date+")"
        
            b=get_data(yuju)
          
        hld_price=hld_price.append(b).reset_index(drop=True)
        
        
    hld_data2=pd.DataFrame()
    hld_data2=pd.merge(hld_data1,hld_price,on='SecuCode',how='outer')
    hld_data2=hld_data2.dropna().reset_index(drop=True)


    hld_data2['Weight']=hld_data2['MarketValue']/sum(hld_data2['MarketValue'])*100
    hld_data2['Weight']=hld_data2['Weight'].apply(lambda x: '%.3f' %x).apply(lambda s: float(s))
    hld_data2['LogReturn']=hld_data2['LogReturn'].apply(lambda s: float(s))
    hld_data2['HldRtn_Prod']=hld_data2['LogReturn']*hld_data2['Weight']
    
    hld=pd.DataFrame()
    hld=hld_data2[['FstIndNameCSRS','HldRtn_Prod','Weight']].groupby(['FstIndNameCSRS']).sum().reset_index()
    hld['HldRtn']=hld['HldRtn_Prod']/hld['Weight']
    hld=hld.dropna().reset_index(drop=True)
    del hld['HldRtn_Prod']

    hld=hld.rename(columns={'FstIndNameCSRS':'Industry'})

    data=pd.DataFrame()
    data=pd.merge(hld,ben,on='Industry',how='outer').rename(columns={'Weight_x':'Weight_hld','Weight_y':'Weight_ben'})
    funddata_all.append(data)


# In[38]:


result=pd.DataFrame()
for i in range(len(funddata_all)):
    funddata_all[i]=funddata_all[i].fillna(0)
    Q=pd.DataFrame()
    Q['Q1'] = funddata_all[i]['Weight_ben'] * funddata_all[i]['BenRtn']
    Q['Q2'] = funddata_all[i]['Weight_hld'] * funddata_all[i]['BenRtn']
    Q['Q3'] = funddata_all[i]['Weight_ben'] * funddata_all[i]['HldRtn']
    Q['Q4'] = funddata_all[i]['Weight_hld'] * funddata_all[i]['HldRtn']
    Q['AR'] = Q['Q2'] - Q['Q1']
    Q['SR'] = Q['Q3'] - Q['Q1']
    Q['IR'] = Q['Q4'] - Q['Q3'] - Q['Q2'] + Q['Q1']
    Q['TR'] = Q['Q4'] - Q['Q1']
    a = pd.DataFrame({'AR': Q['AR'].sum(),
                      'SR': Q['SR'].sum(),
                      'IR': Q['IR'].sum(),
                      'TR': Q['TR'].sum(),
                      'FundCode':fund_code_list[i]},index=[i])
    result=result.append(a)
        
    result=result[['FundCode','AR','SR','IR','TR']]


# In[39]:


result


# In[40]:


result.to_excel('result_p1_zh800.xls')


# # 汇总

# In[48]:


result_hs300=pd.DataFrame()
for i in range(8):
    add='result_p'+ str(i+1) + '_hs300.xls'
    data=pd.read_excel(add,dtype={'FundCode':str})
    result_hs300=result_hs300.append(data).reset_index(drop=True)


# In[52]:


result_hs300.to_excel('result_hs300.xls')


# In[50]:


result_zh800=pd.DataFrame()
for i in range(8):
    add='result_p'+ str(i+1) + '_zh800.xls'
    data=pd.read_excel(add,dtype={'FundCode':str})
    result_zh800=result_zh800.append(data).reset_index(drop=True)


# In[53]:


result_zh800.to_excel('result_zh800.xls')

