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

instance_name = "dbtest"
username = "admin"
host = "dbtest.czb8lzsynwcz.us-east-2.rds.amazonaws.com"
password = "12345678"
port = 3306

#Create Connection to AWS
db = pymysql.connect(user = username, password = password, host = host)
crsr = db.cursor()

sql = '''use livedb'''
crsr.execute(sql)

sql = """SELECT * FROM comment_table WHERE time_created > '2021-07-09:20'"""
crsr.execute(sql)
lst = crsr.fetchall()

sql = """SELECT * FROM post_table WHERE time_created > '2021-07-09:20'"""
crsr.execute(sql)
lst2 = crsr.fetchall()

crsr.close()
db.close()

print(len(lst))
print(len(lst2))
