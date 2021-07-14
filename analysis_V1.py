import datetime
import os
from datetime import datetime as dt
from datetime import timedelta

import pandas as pd
import pymysql
from yahoo_fin import *
from yahoo_fin import stock_info as si
from yahoo_fin.stock_info import *

import ticker_analysis as tc

#Collect database credentials
host = os.environ.get('DB HOST')
password = os.environ.get('DB PASSWORD')
username = os.environ.get('DB USERNAME')


connection = pymysql.connect(user = username, password = password, host = host)
cursor = connection.cursor()
cursor.execute("""USE livedb""")

cursor.execute("""SELECT time_created, tickers 
                FROM post_table 
                WHERE NOT tickers = 'NO TICKERS' AND time_created > '2021-07-13:20'""")

posts = cursor.fetchall()


cursor.execute("""SELECT time_created, tickers 
                FROM comment_table 
                WHERE NOT tickers = 'NO TICKERS' AND time_created > '2021-07-13:20'""")

comments = cursor.fetchall()

cursor.close()
connection.close()


#Combine all index tickers into one
ticker_list = set(si.tickers_sp500() + si.tickers_other() + si.tickers_nasdaq() + si.tickers_dow())
ticker_list.remove('')


#Merge post and comment data
data = comments+posts

#Convert sql object into nested list
data = tc.rowCleaner(data)

#Create dataframe for ticker occurences 
minute_df = tc.data_dataframe(data, 'minute')
hourly_df = tc.data_dataframe(data, 'hourly')
daily_df = tc.data_dataframe(data, 'daily')

print(tc.currentDayLeaders(minute_df, 5))