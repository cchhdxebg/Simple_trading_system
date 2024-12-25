# This is a sample Python script.
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import xlrd
import pandas
import os
import pyodbc
import json
import traceback
from fubon_neo.sdk import FubonSDK, Order
from datetime import datetime
import time
import xlrd
import pandas
import os
import pyodbc
import csv
import pandas as pd
import requests
import smtplib
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def fubon_connect():
    csv_path = ""
    with open(csv_path, newline="") as csvfile:
        para_reader = csv.reader(csvfile, delimiter=',')
        df = pd.DataFrame(para_reader)
        df.columns = df.iloc[0]
        df.set_index('Parameter', inplace=True)
        acc= df._get_value('Fubon_acc', 'Value')
        pwd=df._get_value('Fubon_pwd', 'Value')
        voucher=df._get_value('Fubon_voucher', 'Value')
        voucher_pwd=df._get_value('Fubon_voucher_pwd', 'Value')

        connect_str =[acc,pwd, voucher,voucher_pwd]  # 需登入後，才能取得行情權限
        # from fubon_neo.constant import TimeInForce, OrderType, PriceType, MarketType, BSAction
        sdk = FubonSDK()
        accounts = sdk.login(connect_str[0], connect_str[1], connect_str[2], connect_str[3])  # 若有歸戶，則會回傳多筆帳號資訊
        #acc = accounts.data[0]
        return connect_str,accounts,sdk

def local_sqldb_conn():
    csv_path = "C:\\Users\\Fish\\Downloads\\Python package\\Python Connection parameters.csv"
    with open(csv_path, newline="") as csvfile:
        para_reader = csv.reader(csvfile, delimiter=',')
        df = pd.DataFrame(para_reader)
        df.columns = df.iloc[0]
        df.set_index('Parameter', inplace=True)
        db_server = df._get_value('LocaL_db_server', 'Value')
        db = df._get_value('LocaL_db', 'Value')
        acc = df._get_value('LocaL_db_user', 'Value')
        pwd = df._get_value('Local_db_pwd', 'Value')

        connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=localhost\\SQLEXPRESS;DATABASE={db};UID={acc};PWD={pwd};Trusted_Connection=yes;'
        conn = pyodbc.connect(connectionString)
        cursor = conn.cursor()
        return conn,cursor


def send_email(Subject,msg):
    content = MIMEMultipart()  #建立MIMEMultipart物件
    csv_path = "C:\\Users\\Fish\\Downloads\\Python package\\Python Connection parameters.csv"
    with open(csv_path, newline="") as csvfile:
        para_reader = csv.reader(csvfile, delimiter=',')
        df = pd.DataFrame(para_reader)
        df.columns = df.iloc[0]
        df.set_index('Parameter', inplace=True)
        mailbox = df._get_value('mail', 'Value')
        ppd = df._get_value('gpwd', 'Value')
        content["subject"] = Subject  #郵件標題
        content["from"] = mailbox  #寄件者
        content["to"] = mailbox #收件者
        content.attach(MIMEText(msg))  #郵件內容
        with smtplib.SMTP(host="smtp.gmail.com", port="587") as smtp:  # 設定SMTP伺服器
            try:
                smtp.ehlo()  # 驗證SMTP伺服器
                smtp.starttls()  # 建立加密傳輸
                smtp.login(mailbox, ppd)  # 登入寄件者gmail
                smtp.send_message(content)  # 寄送郵件
                print("Complete!")
            except Exception as e:
                print("Error message: ", e)

#send_email('just test','success \n Process finished with exit code 1')
