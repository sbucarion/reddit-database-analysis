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


def rowCleaner(raw_lst):
    """Takes in a raw list pulled from sql databases and
        converts the items into a list"""
    lst = []
    for item in raw_lst:
        item = list(item)
        lst.append(item)
        
    return lst


def removeNesting(nested_lst):
    """Remove a nesting in a nested list mainly for
        the rowCleaner function. Main use case
        is for the blacklist"""
    
    new_lst = []
    for tuple_ in nested_lst:
        new_lst.append(tuple_[0])
    return new_lst 


def convertUnix(unix_time):
    """Converts praw api unix time to a date time 
        and returns a new value"""
    
    date = dt.fromtimestamp(unix_time).strftime('%Y-%m-%d:%H:%M')
    return date


def archived(date):
    """Checks if a date time is less than n days
        old and returns yes if true"""
    n = 5
    
    today = datetime.datetime.now()
    n_prev_days = datetime.timedelta(days = n)
    archive_date = str(today - n_prev_days)
    
    if date > archive_date: return 'NO'
    else: return 'YES'

    
def handleDate(unix_date):
    """Takes in a unix date and performs the 
        convertUnix and archive function
        and returns both outputs"""
    
    str_date = convertUnix(unix_date)
    archive = archived(str_date)
    
    return str_date, archive


def mergeText(title, selftext):
    """Merges a posts title and body into one piece of text"""
    
    merged_text = title + ". " + selftext
    merged_text = merged_text.replace("'", "")
    return merged_text


def stripText(raw_title):
    """Strips a text of all punctuation and return the clean string"""
    
    letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ 0123456789'
    stripped = ""
    
    for char in raw_title:
        if char in letters:
            stripped = stripped + char
    return stripped


def findTickers(text, ticker_list):
    """Returns a string of tickers found in the text 
        seperated by forward slashes"""
    tickers = []
    
    split = text.split()
    split = set(split)
    for item in split:
        if item in ticker_list:
            item2 = ""
            item2 +=  item + '/'
            tickers.append(item2)
            
    tickers = "".join(tickers)
    
    if len(tickers) == 0:
        tickers = 'NO TICKERS'
    
    return tickers



def streamComments(connection, cursor, subreddit, blacklist = None, ticker_list = None):
    """Streams subreddit comments and adds the comment info to the database
        as long a the author of the comment is not blacklisted. Uses a error
        handling to re run the function in case of connection issues"""

    try:
        for comment in subreddit.stream.comments(skip_existing=True):
            comment_creator = comment.author            

            if comment_creator not in blacklist:
                comment_id = str(comment.id)
                post_id = str(comment.link_id)            

                #Manipulate Text of the comment
                body = str(comment.body)
                body = body.replace("'", "")
                body = stripText(body)


                comment_tickers = findTickers(body, ticker_list)

                comment_upvote = int(comment.score)

                #Turn the time given as a unix into a datetime and check if it needs to be archived
                comment_time = comment.created_utc
                comment_time, isArchived = handleDate(comment_time)

                sql = ("""INSERT INTO comment_table 
                        (comment_id, post_id, creator, comment_body, tickers, sentiment, upvotes, time_created, archived)
                        VALUES('%s','%s','%s','%s','%s', %i, %i, '%s','%s')""" % (comment_id, 
                                                                                    post_id, 
                                                                                    comment_creator, 
                                                                                    body, 
                                                                                    comment_tickers , 
                                                                                    2,
                                                                                    comment_upvote, 
                                                                                    comment_time, 
                                                                                    isArchived))

                cursor.execute(sql)
                connection.commit()

    except Exception as e:
        streamComments(connection, cursor, subreddit, blacklist, ticker_list)             
    
    
def streamPosts(connection, cursor, subreddit, blacklist = None, ticker_list = None):
    """Streams subreddit posts and adds the post info to the database
        as long a the author of the post is not blacklisted. Uses a error
        handling to re run the function in case of connection issues"""   
    
    try:
        for submission in subreddit.stream.submissions(skip_existing=True):
            post_creator = str(submission.author)
            
            if post_creator not in blacklist:
                id_ = str(submission.id)

                post_title = str(submission.title)
                post_selftext = str(submission.selftext)
                
                #Manipulate post text
                post_text = stripText(mergeText(post_title,post_selftext))

                post_tickers = findTickers(post_text, ticker_list)

                post_upvote = int(submission.score)
                comment_count = int(submission.num_comments)

                #Turn the time given as a unix into a datetime and check if it needs to be archived
                post_time = submission.created_utc
                post_time, isArchived = handleDate(post_time)

                sql = ("""INSERT INTO post_table 
                        (post_id,creator,post_body,tickers,comment_count,sentiment,upvotes,time_created,archived)
                        VALUES('%s','%s','%s','%s', %i, %i, %i, '%s','%s')""" % (id_,
                                                                                  post_creator,
                                                                                  post_text,
                                                                                  post_tickers,
                                                                                  comment_count,
                                                                                  2,
                                                                                  post_upvote,
                                                                                  post_time, 
                                                                                  isArchived))
                
                cursor.execute(sql)
                connection.commit()
                
    except Exception as e:
        streamPosts(connection, cursor, subreddit, blacklist, ticker_list)

        
if __name__ == '__main__':
    streamComments(connection, cursor, subreddit, blacklist, ticker_list)
        
if __name__ == '__main__':
    streamPosts(connection, cursor, subreddit, blacklist, ticker_list)       