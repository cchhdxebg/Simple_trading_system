import xlrd
import pandas as pd
import os
import pyodbc
from other_function import *
import holidays
import datetime as dt
import bs4
import requests
import urllib.parse
import json
import csv
from datetime import date, datetime, timezone, timedelta
import time
import numpy as np
from io import StringIO
from urllib.request import urlretrieve
import unicodecsv
from jinja2.nodes import Break
from main import *
from get_whole_TWSE_today_daily_price import *
from dateutil.relativedelta import relativedelta
import fubon_neo
import functools
import matplotlib
import mplfinance as mpf
import pandas.io.sql as psql
import sys
import traceback
import talib
from talib import abstract
import warnings
# Initialize holidays
warnings.filterwarnings(action="ignore")#,category="UserWarning"

# Initialize holidays
tw_holidays = holidays.TW()  # Select country

#TT_date = f"{(T_date.year - 1911)}/{T_date.strftime('%m/%d')}"  #民國年(yyy/mm/dd)

def get_stock_price_lastest_dt_whole_twse(the_dt):
  print('twse start')
  # log_message('twse start')
  url = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data'#這樣拿到的是CSV
  column_list=['Stock_ID','Stock_Name','Volume','Amount','Opening_Price','Highest_Price','Lowest_Price','Closing_Price','Spread','Transactions']
  #證券代號,證券名稱,成交股數,成交金額,開盤價,最高價,最低價,收盤價,漲跌價差,成交筆數
  data = pd.read_csv(url)
  data.columns = column_list#must before dataframe
  df=pd.DataFrame(data)
  df.fillna(0,inplace=True)#must add inplace=True
  insert_conn, cursor = local_sqldb_conn()
  new_dt=datetime.strptime(the_dt, '%Y-%m-%d')
  if len(df) >= 1 :
    for index, row in df.iterrows():
      # nan = np.nan
      # if row.volume == 'nan' or row.volume == nan or  row.open == 'nan' or row.open == nan: #before df.fillna()
      #   continue

      if 1==1:
        # [Stock_ID], [Date], [Period], [volume], [Amount], [open], [high], [low], [close], [Direction], [Spread], [Transactions], [Amplitude], [Extent]
        try:
          cursor.execute("INSERT INTO dbo.Price_Data values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                         row.Stock_ID, new_dt, 'Daily', float(row.Volume),float(row.Amount),float(row.Opening_Price)
                         ,float(row.Highest_Price),float(row.Lowest_Price),float(row.Closing_Price), None, row.Spread
                         ,float(row.Transactions), None, None)
          insert_conn.commit()


        except:
          errorvalue = ('Stock_ID:' + str(row.Stock_ID) + ', the_dt:' + the_dt
                        + ', Volume:' + str(row.Volume) + ',Amount:' + str(row.Amount) + ',Opening_Price:' + str(row.Opening_Price) + ',Highest_Price:' + str(row.Highest_Price)
                        + ',Lowest_Price:' + str(row.Lowest_Price) + ',Closing_Price:' + str(row.Closing_Price) + ',Spread:' + str(row.Spread) + ',Transactions:' + str(row.Transactions))
          cursor.execute("INSERT INTO dbo.Log values(getdate(),?,?)", 'dbo.Price_Data', errorvalue)

          print(errorvalue)

  cursor.close()
  insert_conn.close()
  print('twse end')


def get_stock_price_lastest_dt_whole_otc(lastest_dt):
  print('otc start')
  #url = 'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes?response=json'  #TradingShares 在20241215 比對成交量 很不準
  url = 'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes?response=json'  #TradingShares 在20241215 比對成交量 比較準 not work-->&date=+str(t_date)+'&stockNo='+str(Stock_ID)
  response = requests.get(url)  # 到這 OK
  column_list = ['Date', 'SecuritiesCompanyCode', 'CompanyName', 'Close', 'Change', 'Open', 'High',
                 'Low','Average', 'TradingShares','TransactionAmount','TransactionNumber','LatestBidPrice','LatesAskPrice','Capitals','NextLimitUp','NextLimitDown']
  #TradingShares 成交量 , Change漲跌,TransactionNumber成交筆數,LatestBidPrice最後買價,LatesAskPrice最後賣價,Capitals發行股數
  #in db column_list= [Stock_ID], [Date], [Period], [Volume], [Amount], [Opening_Price], [Highest_Price], [Lowest_Price], [Closing_Price], [Direction], [Spread], [Transactions], [Amplitude], [Extent]
  response = requests.get(url)
  response_data = response.json()
  df = pd.DataFrame(response_data)
  df.fillna(0,inplace=True)
  df.replace('----', 0,inplace=True)#must add inplace=True
  df.replace('---', 0,inplace=True)


  insert_conn, insert_cursor = local_sqldb_conn()
  if len(df.columns) == 16:  # holiday not have data--->means have data
    #print('in')
    #following line was added before df.replace('---', 0,inplace=True)
    df['volume'] = np.where(df['TradingShares']=='----',0,df['TradingShares'])
    df['amount'] = np.where(df['TransactionAmount']=='----',0,df['TransactionAmount'])
    df['transactions'] = np.where(df['TransactionNumber']=='----',0,df['TransactionNumber'])
    df['direction'] = np.where(df['Change']=='---',0,df['Change'])  # JUST ---!!!
    df['Open_1'] = np.where(df['Open']=='----',0,df['Open'])
    df['High_1'] = np.where(df['High']=='----',0,df['High'])
    df['Low_1'] = np.where(df['Low']=='----',0,df['Low'])
    df['Close_1'] = np.where(df['Close']=='----',0,df['Close'])

    for index, row in df.iterrows():
      # [Stock_ID], [Date], [Period], [Volume], [Amount], [Opening_Price], [Highest_Price], [Lowest_Price], [Closing_Price]
      # , Change=[Direction], [Spread], [Transactions], [Amplitude], [Extent]
      try:
        the_dt = str(int(row.Date[0:3]) + 1911) + row.Date[3:10]#must 民國年 to 西元 YYYYMMDD
        volume = row.volume
        amount = row.amount
        transactions = row.transactions
        direction = row.direction
        open = row.Open_1
        High = row.High_1
        Low = row.Low_1
        Close =row.Close_1

        if (str(transactions).rstrip()=="0") or (str(volume).rstrip()=="0") or (str(amount).rstrip()=="0") or (Close==0):
          direction=None
          open=0
          High=0
          Close=0
          Low=0
          amount=0
          volume=0

        volume = float(volume) / 1000

        insert_cursor.execute("IF(SELECT count(1) FROM [Market].[dbo].[TW_STOCK_LIST] where [有價證券代號]=? )=1 begin INSERT INTO dbo.Price_Data values(?,?,?,?,?,?,?,?,?,?,?,?,?,?) end",
          str(row.SecuritiesCompanyCode),str(row.SecuritiesCompanyCode), str(the_dt), 'Daily', float(volume),float(amount)
          ,float(open),float(High), float(Low),float(row.Close), str(direction), None
          ,float(transactions), None, None)
        insert_conn.commit()

      except Exception as inst:
        print(type(inst))  # the exception type
        print(inst.args)  # arguments stored in .args
        print(inst)  # __str__ allows args to be printed directly,
        # but may be overridden in exception subclasses
        errorvalue = ('Stock_ID:' + str(row.SecuritiesCompanyCode) + ', the_dt:' + str(the_dt)+ ', Volume:' + str(row.TradingShares) + ',Amount:'
                      + str(row.TransactionAmount) + ',Opening_Price:' + str(row.Open) + ',Highest_Price:' + str(row.High)
                      +',Lowest_Price:' + str(row.Low) + ',Closing_Price:' + str(row.Close) + ',direction:' + str(row.Change) + ',Transactions:' + str(row.TransactionNumber)
                      )
        # cursor.execute("INSERT INTO dbo.Log values(getdate(),?,?)", 'dbo.Price_Data', errorvalue)\
        df_error=df[df['SecuritiesCompanyCode']==row.SecuritiesCompanyCode]
        print(df_error[:17])
        print(errorvalue)

  insert_cursor.close()#must above conn.close()
  insert_conn.close()
  print('otc end')


def correct_stock_daily_data():
  print('correct_stock_daily_data start')
  sql_query = """
    exec [Market].[dbo].[price_data_correct]
    """
  Correct_conn, Correct_cursor = local_sqldb_conn()
  Correct_cursor.execute(sql_query)
  Correct_cursor.close()
  Correct_conn.close()
  print('correct_stock_daily_data end')

def get_daily_indicators(the_dt):
  print('get_daily_indicators start')
  # log_message('get_daily_indicators start')
  try:
    # step 1 GET all twse stock list
    SQL_QUERY = """
      SELECT  
      [有價證券代號] as Stock_ID,[上市日] as min_dt
      FROM [Market].[dbo].[TW_STOCK_LIST] as A 
      where 1=1
      and [市場別]='上市' 
      order by 1 DESC
      """

    get_conn, get_cursor = local_sqldb_conn()
    get_cursor.execute(SQL_QUERY)
    Stock_ID = ''
    records = get_cursor.fetchall()
    # 一次只計算一個股
    for r in records:
      # step 2 every stock_ID have to search missing data exists or not
      Stock_ID = str(r.Stock_ID)
      the_dt = str(the_dt)
      # 112->YYYYMMDD
      get_conn_1, get_cursor_1 = local_sqldb_conn()
      SQL_QUERY_1 = "SELECT * FROM [Market].[dbo].[Price_Data] where [Stock_ID]='" + Stock_ID + "' and convert(date,[date])>=dateadd(week,-60,convert(date,'" + the_dt + "')) order by 2"
      df = psql.read_sql(SQL_QUERY_1, get_conn_1)
      # column_list = ['Stock_ID', 'date', 'Period', 'Volume', 'Amount', 'Open', 'High', 'Low', 'Close', 'Direction', 'Spread', 'Transactions', 'Amplitude', 'Extent']
      if len(df) == 0:
        raise Exception('no data,Stock_ID:' + Stock_ID)

      # print(df.head(3))
      df.set_index('date', inplace=True)
      # print(Stock_ID)
      df.sort_index(inplace=True)  # not work -->add ascending=True

      # 計算KD值
      df_kd = abstract.STOCH(df.sort_index(ascending=True), fastk_period=9, slowk_period=3, slowd_period=3)
      # date,slowk,slowd,K_D_signal
      # ['date','K','D']=df_kd.
      df_kd.fillna(0, inplace=True)
      # K值大於D值，表示為1，反之為0
      df_kd['K_D_signal'] = np.where(df_kd['slowk'] - df_kd['slowd'] > 0, 1, 0)
      df_kd.reset_index(inplace=True)

      # 計算MACD值
      df_macd = abstract.MACD(df.sort_index(ascending=True), fastperiod=12, slowperiod=26, signalperiod=9)
      df_macd.fillna(0, inplace=True)
      # macd, macdsignal, macdhist，分別就是 快線、慢線、柱狀圖
      # DIF, MACD, OSC，分別就是 快線、慢線、柱狀圖
      # macd的 dif12-26 值大於macdsignal值，表示為1，反之為0
      df_macd['macd_color'] = df_macd['macdhist'].apply(lambda x: 1 if x >= float(0) else 1)
      # 柱狀圖>0 return 1 is red, <0 is green
      df_macd.reset_index(inplace=True)

      ## 創建布林通道：  週期 20日（＝日K月均線）、2個標準差
      df_bb = abstract.BBANDS(df, timeperiod=20, nbdevup=2.0, nbdevdn=2.0, matype=0)
      df_bb.reset_index(inplace=True)
      # df_bb.column=['date','U_band','M_band','L_band']
      # columns upperband  middleband  lowerband

      # 均價

      df['Price_5MA'] = df['close'].rolling(5).mean()
      df['Price_10MA'] = df['close'].rolling(10).mean()
      df['Price_20MA'] = df['close'].rolling(20).mean()
      df['Price_60MA'] = df['close'].rolling(60).mean()
      df['Price_120MA'] = df['close'].rolling(120).mean()
      df['Price_240MA'] = df['close'].rolling(240).mean()

      # 乖離率
      df['BIAS20'] = round((df['close'] - df['close'].rolling(20, min_periods=1).mean()) / df['close'].rolling(20,min_periods=1).mean() * 100,2)
      df['BIAS60'] = round((df['close'] - df['close'].rolling(60, min_periods=1).mean()) / df['close'].rolling(60,min_periods=1).mean() * 100,2)
      df['BIAS240'] = round((df['close'] - df['close'].rolling(240, min_periods=1).mean()) / df['close'].rolling(240,min_periods=1).mean() * 100,2)

      # MTM
      MTM = 10
      MTM_MA = 10
      df['MTM'] = df['close'] - df['close'].shift(MTM)
      df['MTM_MA'] = df['MTM'].rolling(MTM_MA).mean()
      df['MTM_signal'] = np.where(df['MTM'] - df['MTM_MA'] > 0, 1, 0)
      # MTM,MTM_MA,MTM_signal
      # rolling(10).mean()10日移動平均線
      # 滾動20日高點 =df.rolling(20).max()

      # 均量
      df['Vol_5MA'] = df['volume'].rolling(5).mean()
      df['Vol_10MA'] = df['volume'].rolling(10).mean()
      df['Vol_20MA'] = df['volume'].rolling(20).mean()
      df['Vol_60MA'] = df['volume'].rolling(60).mean()

      ## RSI
      df['RSI6'] = abstract.RSI(df, 6)
      df['RSI12'] = abstract.RSI(df, 12)
      df['RSI_signal'] = np.where(df['RSI6'] - df['RSI12'] > 0, 1, 0)

      ####merge
      df_1 = pd.merge(df, df_kd, on='date', how='left')
      df_1.drop_duplicates(subset=['date'], keep='first', inplace=True)
      df_2 = pd.merge(df_1, df_macd, on='date', how='left')
      df_2.drop_duplicates(subset=['date'], keep='first', inplace=True)
      df_3 = pd.merge(df_2, df_bb, on='date', how='left')
      df_3.drop_duplicates(subset=['date'], keep='first', inplace=True)
      df_3.fillna(0, inplace=True)
      df_3['FI_Balance'] = 0
      df_3['FI_Hold'] = 0
      df_3['IT_Balance'] = 0
      df_3['IT_Hold'] = 0
      df_3['Dealer_Balance'] = 0
      df_3['Dealer_Hold'] = 0

      new_dt = datetime.strptime(the_dt, '%Y-%m-%d')
      # df_4=df_3.query('[date]=='+the_dt) #NOT WORK
      df_4 = df_3[df_3['date'] == new_dt]
      if len(df_4) > 1:
        raise Exception('duplicate data still exists')

      if len(df_4) == 1:
        insert_conn, cursor = local_sqldb_conn()
        for index, row in df_4.iterrows():
          try:
            # ts=row.time*0.000001
            # tn = datetime.fromtimestamp(ts)
            # tn_str=str(tn)
            cursor.execute(
              "INSERT INTO [Market].[dbo].[Price_Data_indicator_Daily] values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
              Stock_ID, row.date, float(row.Price_5MA), float(row.Price_10MA), float(row.Price_20MA),
              float(row.Price_60MA), float(row.Price_120MA), float(row.Price_240MA)
              , float(row.BIAS20), float(row.BIAS60), float(row.BIAS240), float(row.MTM), float(row.MTM_MA),
              row.MTM_signal
              , float(row.Vol_5MA), float(row.Vol_10MA), float(row.Vol_20MA), float(row.Vol_60MA)
              , float(row.slowk), float(row.slowd), float(row.K_D_signal)
              , float(row.macd), float(row.macdsignal), float(row.macdhist), row.macd_color, float(row.RSI6),
              float(row.RSI12), row.RSI_signal
              , float(row.upperband), float(row.middleband), float(row.lowerband)
              , row.FI_Balance, row.FI_Hold, row.IT_Balance, row.IT_Hold, row.Dealer_Balance, row.Dealer_Hold
              )

            insert_conn.commit()

          except Exception as e:
            print(e)
            errorvalue = ('Stock_ID:' + str(Stock_ID) + ', the_dt:' + str(row.date)
                          + ',Price_5MA:' + str(row.Price_5MA) + ',Price_10MA:' + str(
                      row.Price_10MA) + ',Price_20MA:' + str(row.Price_20MA) + ',Price_60MA:' + str(
                      row.Price_60MA) + ',Price_Price_120MA:' + str(row.Price_120MA) + ',Price_240MA:' + str(
                      row.Price_240MA)
                          + ',BIAS20:' + str(row.BIAS20) + ',BIAS60:' + str(row.BIAS60) + ',BIAS240:' + str(
                      row.MTM) + ',BIAS60:' + str(row.MTM) + ',MTM_MA:' + str(row.MTM_MA) + ',MTM_signal:' + str(
                      row.MTM_signal)
                          + ',Vol_5MA:' + str(row.Vol_5MA) + ',Vol_10MA:' + str(row.Vol_10MA) + ',Vol_20MA:' + str(
                      row.Vol_20MA) + ',Vol_60MA:' + str(row.Vol_60MA)
                          + ',slowk:' + str(row.slowk) + ',Vol_10MA:' + str(row.slowd) + ',K_D_signal:' + str(
                      row.K_D_signal)
                          + ',macd:' + str(row.macd) + ',macdsignal:' + str(row.macdsignal) + ',macdhist:' + str(
                      row.macdhist) + ',macd_color:' + str(row.macd_color) + ',RSI6:' + str(row.RSI6) + ',RSI12:' + str(
                      row.RSI12) + ',RSI_signal:' + str(row.RSI_signal)
                          + ',upperband:' + str(row.upperband) + ',middleband:' + str(
                      row.middleband) + ',lowerband:' + str(row.lowerband)
                          + ',FI_Balance:' + str(row.FI_Balance) + ',FI_Hold:' + str(
                      row.FI_Hold) + ',IT_Balance:' + str(row.IT_Balance) + ',IT_Hold:' + str(
                      row.IT_Hold) + ',Dealer_Balance:' + str(row.Dealer_Balance) + ',Dealer_Hold:' + str(
                      row.Dealer_Hold)
                          )
            cursor.execute("INSERT INTO dbo.Log values(getdate(),?,?)", 'dbo.Price_Data_indicator_Daily', errorvalue)

            print(errorvalue)
            insert_conn.close()
            cursor.close()
      else:
        raise Exception('blank datafrfame')

  except Exception as e:
    print(e)
  # log_message('get_daily_indicators end')
  print('get_daily_indicators end')

def daily_check(the_dt):
  print('stock_daily_check start')
  conn, cursor = local_sqldb_conn()
  email_title='Daily check on '+the_dt
  email_msg='hi~\n here is check result :'
  try:
    sql_query = "exec [dbo].[Daily_Check] ?"
    #df = pd.read_sql(sql_query, conn)
    cursor.execute(sql_query,the_dt)
    Stock_ID = ''
    records = cursor.fetchall()
    for r in records:
      email_msg=email_msg+'\n '+r.tablename+' has '+str(r.counts)

    #get miss stock list
    sql_query = "exec [dbo].[find_today_miss_stock] ?"
    #df = pd.read_sql(sql_query, conn)
    cursor.execute(sql_query, the_dt)
    records = cursor.fetchall()
    if len(records)==1:
      email_msg=the_dt+'is holiday'
    else:
      pre_tablename=''
      for r in records:
        if r.tablename==pre_tablename:
          email_msg = email_msg +',' + str(r.missing_stock_id)
        else:
          email_msg = email_msg + '\n ' + r.tablename + ' misses ' + str(r.missing_stock_id)

        pre_tablename=r.tablename#reset

  except Exception as e:
    print(e)
  cursor.close()
  conn.close()
  send_email(email_title, email_msg)
  print('stock_daily_check end')

def send_error():
  sql_query="""
  select count(1) counts from market.dbo.log where convert(date,log_time)= convert(date,getdate())
  """
  insert_conn, cursor = local_sqldb_conn()
  cursor.execute(sql_query)
  record = cursor.fetchone()
  cursor.close()
  insert_conn.close()

  if record.counts>=1:
    print('抓股價資料有誤！請看LOG')


