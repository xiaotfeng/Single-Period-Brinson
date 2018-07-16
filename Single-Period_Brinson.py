

import pandas as pd
import numpy as np
import datetime
import datetime
import dateutil

from dateutil.parser import parse
import os,pymssql
import re
import math

#链接恒生聚源数据库

def get_data(sql1):
    server = "xxx.xxx.xxx.xx"
    user = "xx"
    password = "xxx"
    conn = pymssql.connect(server,user,password,database="xxxx",charset='utf8')
    cursor = conn.cursor()
    cursor.execute(sql1)
    row = cursor.fetchall()
    conn.close()
    data = pd.DataFrame(row,columns=zip(*cursor.description)[0])
    data = l2gbyR(data)
    return data
    
def get_last_season_enddate(latest_date):
    #获取上一个season的endDate
    #input必须为pandas的datetime变量
    year=latest_date.year
    month=latest_date.month
    
    if month in [3,6]:
        day = '31'
    else:
        day = '30'
        
    if month == 3:
        year = str(year-1)
        month = str(12)
    else:
        year = str(year)
        month = '0'+str(month-3)
        
    last_season_end_date = "'"+year+'-'+month+'-'+day+"'"
    
    return last_season_end_date
    
#-----------------------------------------------------------------------------    
#基金数据清洗
#中欧时代先锋A代码：001938
#读取基金年报持仓数据
hld_data=get_data("SELECT b.SecuCode as FundCode,b.SecuAbbr as FundName,\
                  c.SecuCode,c.SecuAbbr, a.SharesHolding,a.MarketValue,a.ReportDate\
                  FROM MF_StockPortfolioDetail a,SecuMain b,SecuMain c \
                  WHERE a.InnerCode=b.InnerCode and b.SecuCode='001938' \
                  and a.StockInnerCode=c.InnerCode and \
                  a.ReportDate=(select MAX(c.ReportDate) from MF_StockPortfolioDetail c where a.InnerCode=c.InnerCode)")

#读取基金持仓行业名称
a=pd.DataFrame()
hld_industryname=pd.DataFrame()

for code in range(len(hld_data['SecuCode'])):
    i="'"+str(hld_data['SecuCode'][code])+"'"
    
    yuju="SELECT b.SecuCode,a.FstIndNameCSRS from LC_CSIIndustry a, SecuMain b \
    where a.InnerCode=b.InnerCode and b.SecuCode="+ i +" and \
    a.EndDate=(select max(c.EndDate) from LC_CSIIndustry c where a.InnerCode=c.InnerCode)"
    
    a=get_data(yuju)
    hld_industryname=hld_industryname.append(a).reset_index(drop=True)

#将基金基础数据与行业数据merge为hld_data1
hld_data1=pd.merge(hld_data,hld_industryname)


#读取并计算头尾持仓股价收益率
b=pd.DataFrame()
hld_price=pd.DataFrame()

for code in range(len(hld_data['SecuCode'])):
    i="'"+str(hld_data['SecuCode'][code])+"'"
    hld_data1['ReportDate']=pd.to_datetime(hld_data1['ReportDate'])
    current_season_date=hld_data1.iloc[0].loc['ReportDate']
    last_season_date=get_last_season_enddate(current_season_date)
    current_season_date="'"+str(current_season_date)+"'"
    
    try:
        
        yuju="SELECT c.SecuCode, a.OpenPrice as BgnPeriodPrice,\
        b.ClosePrice as EndPeriodPrice,LOG(b.ClosePrice/a.OpenPrice) as log_return \
        FROM QT_DailyQuote a,QT_DailyQuote b,SecuMain c,SecuMain d \
        WHERE c.SecuCategory=1 and d.SecuCategory=1 AND a.InnerCode=c.InnerCode \
        AND b.InnerCode=d.InnerCode AND c.SecuCode="+ i +" AND d.SecuCode="+ i +" \
        AND b.TradingDay=(select Max(e.TradingDay) FROM QT_DailyQuote e,SecuMain i \
        WHERE e.InnerCode=i.InnerCode AND i.SecuCategory=1 AND i.SecuCode="+ i +" \
        AND e.TradingDay <="+ current_season_date +") AND \
        a.TradingDay=(select MIN(h.TradingDay) FROM QT_DailyQuote h,SecuMain j \
        WHERE h.InnerCode=j.InnerCode AND j.SecuCategory=1 AND j.SecuCode="+ i +" \
        AND h.TradingDay > "+last_season_date+")"
        
        b=get_data(yuju)
    
    except :
        yuju="SELECT c.SecuCode, a.ClosePrice as BgnPeriodPrice,\
        b.ClosePrice as EndPeriodPrice, LOG(b.ClosePrice/a.ClosePrice) as log_return \
        FROM QT_DailyQuote a,QT_DailyQuote b,SecuMain c,SecuMain d \
        WHERE c.SecuCategory=1 and d.SecuCategory=1 AND a.InnerCode=c.InnerCode \
        AND b.InnerCode=d.InnerCode AND c.SecuCode="+ i +" AND d.SecuCode="+ i +" \
        AND b.TradingDay=(select Max(e.TradingDay) FROM QT_DailyQuote e,SecuMain i \
        WHERE e.InnerCode=i.InnerCode AND i.SecuCategory=1 AND i.SecuCode="+ i +" \
        AND e.TradingDay <="+ current_season_date +") AND \
        a.TradingDay=(select MIN(h.TradingDay) FROM QT_DailyQuote h,SecuMain j \
        WHERE h.InnerCode=j.InnerCode AND j.SecuCategory=1 AND j.SecuCode="+ i +" \
        AND h.TradingDay > "+last_season_date+")"
        
        b=get_data(yuju)
          
    hld_price=hld_price.append(b).reset_index(drop=True)

hld_data2=pd.merge(hld_data1,hld_price,on='SecuCode',how='outer')
hld_data2=hld_data2.dropna().reset_index(drop=True)

#-------------------------------------------------------------------------------------------
#基准数据清洗
#中证500代码：000905
#读取离基金报告期最近的指数成分数据
benchmark_data=get_data("SELECT b.SecuCode as IndexCode,b.SecuAbbr as IndexAbbr,\
                        c.SecuCode,c.SecuAbbr,a.Weight,a.EndDate FROM \
                        LC_IndexComponentsWeight a, SecuMain b,SecuMain c WHERE \
                        a.IndexCode=b.InnerCode AND a.InnerCode=c.InnerCode AND \
                        b.SecuCode='000905' and a.EndDate=(select MAX(d.EndDate) \
                        FROM LC_IndexComponentsWeight d WHERE a.IndexCode=d.IndexCode\
                        AND d.EndDate<=(select MAX(e.ReportDate) FROM SecuMain d,MF_StockPortfolioDetail e\
                        WHERE e.InnerCode=d.InnerCode AND d.SecuCode='001938'))")


#读取指数成分行业名称
benchmark_industry=pd.DataFrame()
for code in range(len(benchmark_data['SecuCode'])):
    
    i="'"+str(benchmark_data['SecuCode'][code])+"'"
    
    yuju="SELECT b.SecuCode,a.FstIndNameCSRS FROM LC_CSIIndustry a, SecuMain b \
    WHERE a.InnerCode=b.InnerCode and b.SecuCode="+ i +" AND \
    a.EndDate=(SELECT max(c.EndDate) FROM LC_CSIIndustry c WHERE a.InnerCode=c.InnerCode)"
    
    a=get_data(yuju)
    benchmark_industry=benchmark_industry.append(a).reset_index(drop=True)


#读取并计算指数成分收益率
benchmark_price=pd.DataFrame()

for code in range(len(benchmark_data['SecuCode'])):
    i="'"+str(benchmark_data['SecuCode'][code])+"'"
    hld_data1['ReportDate']=pd.to_datetime(hld_data1['ReportDate'])
    current_season_date=hld_data1.iloc[0].loc['ReportDate']
    last_season_date=get_last_season_enddate(current_season_date)
    current_season_date="'"+str(current_season_date)+"'"
    
    try:
        
        yuju="SELECT c.SecuCode, a.OpenPrice as BgnPeriodPrice,\
        b.ClosePrice as EndPeriodPrice,LOG(b.ClosePrice/a.OpenPrice) as log_return\
        FROM QT_DailyQuote a,QT_DailyQuote b,SecuMain c,SecuMain d WHERE \
        c.SecuCategory=1 and d.SecuCategory=1 AND a.InnerCode=c.InnerCode AND \
        b.InnerCode=d.InnerCode AND c.SecuCode="+ i +" AND d.SecuCode="+ i +" AND \
        b.TradingDay=(select Max(e.TradingDay) FROM QT_DailyQuote e,SecuMain i WHERE \
        e.InnerCode=i.InnerCode AND i.SecuCategory=1 AND i.SecuCode="+ i +" AND \
        e.TradingDay <="+ current_season_date +") AND a.TradingDay=\
        (select MIN(h.TradingDay) FROM QT_DailyQuote h,SecuMain j  WHERE \
        h.InnerCode=j.InnerCode AND j.SecuCategory=1 AND j.SecuCode="+ i +" AND h.TradingDay > "+last_season_date+")"
        
        b=get_data(yuju)
    
    except :
        yuju="SELECT c.SecuCode, a.ClosePrice as BgnPeriodPrice,\
        b.ClosePrice as EndPeriodPrice, LOG(b.ClosePrice/a.ClosePrice) as log_return\
        FROM QT_DailyQuote a,QT_DailyQuote b,SecuMain c,SecuMain d WHERE \
        c.SecuCategory=1 and d.SecuCategory=1 AND a.InnerCode=c.InnerCode AND \
        b.InnerCode=d.InnerCode AND c.SecuCode="+ i +" AND d.SecuCode="+ i +" AND \
        b.TradingDay=(select Max(e.TradingDay) FROM QT_DailyQuote e,SecuMain i WHERE \
        e.InnerCode=i.InnerCode AND i.SecuCategory=1 AND i.SecuCode="+ i +" AND \
        e.TradingDay <="+ current_season_date +") AND a.TradingDay=\
        (select MIN(h.TradingDay) FROM QT_DailyQuote h,SecuMain j  WHERE \
        h.InnerCode=j.InnerCode AND j.SecuCategory=1 AND j.SecuCode="+ i +" AND h.TradingDay > "+last_season_date+")"
        
        b=get_data(yuju)
          
    benchmark_price=benchmark_price.append(b).reset_index(drop=True)
    
    
benchmark_data1=pd.merge(benchmark_data,benchmark_industry,on='SecuCode')
benchmark_data2=pd.merge(benchmark_data1,benchmark_price,on='SecuCode',how='outer').dropna().reset_index(drop=True)




hld_data2['Weight']=hld_data2['MarketValue']/sum(hld_data2['MarketValue'])*100
hld_data2['Weight']=hld_data2['Weight'].apply(lambda x: '%.3f' %x).apply(lambda s: float(s))
hld_data2['log_return']=hld_data2['log_return'].apply(lambda s: float(s))
hld_data2['HldRtn_Prod']=hld_data2['log_return']*hld_data2['Weight']
benchmark_data2['Weight_Adjusted']=benchmark_data2['Weight']/sum(benchmark_data2['Weight'])*100
benchmark_data2['Weight_Adjusted']=benchmark_data2['Weight_Adjusted'].apply(lambda x: '%.3f' %x).apply(lambda s: float(s))
benchmark_data2['log_return']=benchmark_data2['log_return'].apply(lambda s: float(s))
benchmark_data2['BenRtn_Prod']=benchmark_data2['log_return']*benchmark_data2['Weight_Adjusted']
benchmark=benchmark_data2[['FstIndNameCSRS','BenRtn_Prod','Weight_Adjusted']].groupby(['FstIndNameCSRS']).sum().reset_index()
hld=hld_data2[['FstIndNameCSRS','HldRtn_Prod','Weight']].groupby(['FstIndNameCSRS']).sum().reset_index()
hld=hld.rename(columns={'Weight':'HldWeight'})
benchmark=benchmark.rename(columns={'Weight_Adjusted':'BenWeight'})
Q=pd.merge(hld,benchmark,on='FstIndNameCSRS',how='outer')
Q.fillna(0)


Q['HldWeight']=Q['HldWeight']/100
Q['BenWeight']=Q['BenWeight']/100
#1.基准收益基准权重
Q['Q1'] = Q['BenRtn_Prod'] * Q['BenWeight']
#2.基准收益组合权重
Q['Q2'] = Q['BenRtn_Prod'] * Q['HldWeight']
#3.基准权重组合收益
Q['Q3'] = Q['HldRtn_Prod'] * Q['BenWeight']
#4.组合收益组合权重
Q['Q4'] = Q['HldRtn_Prod'] * Q['HldWeight']
Q['AR'] = Q['Q2'] - Q['Q1']
Q['SR'] = Q['Q3'] - Q['Q1']
Q['IR'] = Q['Q4'] - Q['Q3'] - Q['Q2'] + Q['Q1']
Q['TR'] = Q['Q4'] - Q['Q1']
Q['FundCode']=hld_data2['FundCode']
Q_Result=Q[['Q1','Q2','Q3','Q4','AR','SR','IR','TR']]
Q_Result=Q_Result.apply(lambda x: x.sum())
