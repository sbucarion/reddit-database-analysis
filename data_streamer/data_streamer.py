import praw
import threading
from threading import Thread
from datetime import datetime as dt
from datetime import timedelta
import datetime
from yahoo_fin import stock_info as si
from yahoo_fin.stock_info import *
from yahoo_fin import *
import pymysql
import data_collection as dc
import os


#Read a file with tickers that are not commonly used as tickers but rather common words
#Words like YOLO are often used to indicate risky trades and not meant to represent a ticker
bad_tickers = set()
with open("bad_tickers", 'r') as f:
    lines = f.readlines()
    
    for i, ticker in enumerate(lines):
        #Remove the \n at the end of each item
        
        if i != len(lines)-1:            
            bad_tickers.add(ticker[:-1])            
        else:            
            bad_tickers.add(ticker)

#Combine all index tickers into one and remove the bad tickers 
ticker_list = set(si.tickers_sp500() + si.tickers_other() + si.tickers_nasdaq() + si.tickers_dow())
ticker_list = ticker_list - bad_tickers
ticker_list.remove('')


#Pull reddit api credentials from system properties 
client_id = os.environ.get('PRAW CLIENT ID')
client_secret = os.environ.get('PRAW CLIENT SECRET')
user_agent = os.environ.get('PRAW USER AGENT')


#Pull database credentials from system properties 
username = os.environ.get('DB USERNAME')
password = os.environ.get('DB PASSWORD')
host = os.environ.get('DB HOST')


#Create reddit api instance and select specific subreddit
#Initialize praw instance and use the instance to call the wallstreetbets subreddit
reddit = praw.Reddit(client_id = client_id, 
                     client_secret = client_secret, 
                     user_agent = user_agent)
subreddit = reddit.subreddit("wallstreetbets")


#Create Connection to AWS
#Connection for Comment Streamer
connection1 = pymysql.connect(user = username, password = password, host = host)
cursor1 = connection1.cursor()
cursor1.execute("""USE livedb""")


#Connection for Post Steamer
connection2 = pymysql.connect(user = username, password = password, host = host)
cursor2 = connection2.cursor()
cursor2.execute("""USE livedb""")


#Connection for the Blacklist Table
connection3 = pymysql.connect(user = username, password = password, host = host)
cursor3 = connection3.cursor()
cursor3.execute("""USE livedb""")

#Retrieve all data from the blacklist table
cursor3.execute("""SELECT * FROM blacklist""")
blacklist = cursor3.fetchall()

#Convert query from list of tuples to list of strings
blacklist = dc.removeNesting(dc.rowCleaner(blacklist))

cursor3.close()
connection3.close()


print('Running...')

#Run both post streamer and comment streamer at same time with multihreading
Thread(target = dc.streamPosts, args = (connection1, 
                                        cursor1, 
                                        subreddit,
                                        blacklist,
                                        ticker_list,)).start()

Thread(target = dc.streamComments, args = (connection2, 
                                           cursor2, 
                                           subreddit,
                                           blacklist,
                                           ticker_list,)).start()