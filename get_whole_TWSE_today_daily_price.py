import tkinter as tk
from tkinter import ttk, messagebox
import xlrd
import pandas
import os
import holidays
import datetime
import pyodbc
from getStockPriceData import *
from other_function import *
from main import *
import threading
#import MIMEMultipart not a model
from email.mime.multipart import MIMEMultipart
# Global variable to control the execution
stop_event = threading.Event()
execution_thread = None

def run_code():
    try:
        tw_holidays = holidays.TW()  # for checking missdate is holiday
        # log_message('start')
        #today = str(datetime.date.today().strftime('%Y%m%d'))
        conn, cursor = local_sqldb_conn()
        str_market = 'TWSE'  # 以TWSE為主 如果有遇到錯誤，需要檢查後，更新lastest_data_date of [dbo].[system_settings]才能重來
        sql_query = "exec dbo.[get_run_data_date] ?"
        cursor.execute(sql_query, str_market)
        row_to_run = cursor.fetchone()
        to_run = row_to_run[0]
        the_dt = str(row_to_run[1])
        
        if to_run == 1:
            # must execute daily
            print(str_market + ',' + the_dt)
            get_stock_price_lastest_dt_whole_twse(the_dt)
            get_stock_price_lastest_dt_whole_otc(the_dt)
            correct_stock_daily_data()
            get_daily_indicators(the_dt)
            daily_check(the_dt)

            sql_query_1 = f"SET NOCOUNT OFF; exec dbo.[update_lastest_data_date] ?"
            cursor.execute(sql_query_1, the_dt)


        if to_run == 0:
            # log_message("Daily Execution no need to run" + ',' + the_dt)
            send_email(the_dt, "Daily Execution no need to run")

        cursor.close()
        conn.close()
        send_email(the_dt,"Daily Execution completed successfully.")
    except Exception as e:
        print(f"Error: {str(e)}")
        # log_message(f"Error: {str(e)}")
run_code()

# def start_execution():
#     global execution_thread
#     log_message("Starting execution...")
#     stop_event.clear()
#     execution_thread = threading.Thread(target=run_code)
#     execution_thread.start()
#
# def stop_execution():
#     global execution_thread
#     log_message("Stopping execution...")
#     stop_event.set()
#     if execution_thread is not None:
#         execution_thread.join()
#     log_message("Execution stopped.")
#
# def log_message(message):
#     log_text.config(state=tk.NORMAL)
#     log_text.insert(tk.END, message + "\n")
#     log_text.config(state=tk.DISABLED)
#     log_text.see(tk.END)
#
# # Create the main window
# root = tk.Tk()
# root.title("Daily Indicators Execution")
#
# # Create and place the start button
# start_button = ttk.Button(root, text="Start", command=start_execution)
# start_button.grid(row=0, column=0, padx=10, pady=10)
#
# # Create and place the stop button
# stop_button = ttk.Button(root, text="Stop", command=stop_execution)
# stop_button.grid(row=0, column=1, padx=10, pady=10)
#
# # Create and place the log text box
# log_text = tk.Text(root, state=tk.DISABLED, width=80, height=20)
# log_text.grid(row=1, column=0, columnspan=2, padx=10, pady=10)
#
# # Run the main event loop
# root.mainloop()
