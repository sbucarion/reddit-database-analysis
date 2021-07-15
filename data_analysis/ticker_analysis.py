import pandas as pd
import pymysql
import os


def rowCleaner(query):
    """Takes in a raw list pulled from sql databases and
        converts the items into a list"""
    lst = []
    for item in query:
        item = list(item)
        lst.append(item)
        
    for i in range(len(lst)):
        lst[i][1] = "/" + lst[i][1]
        
    return lst


def seperateQuery(query_data):
    """Takes in a query and splits the nested list by column
        and returns each individual column"""
    
    query_tickers = []
    query_dates = []
    
    for date, ticker in query_data:
        query_dates.append(date)
        query_tickers.append(ticker)
        
    return query_dates, query_tickers


def findUniqueTickers(tickers_query):
    """Finds all tickers in the tickers query and returns a 
        set of the tickers"""
    
    set_ = set()
        
    for item in tickers_query:
        split = item.split("/")
        split.remove('')
        
        for item in split:
            if item not in set_ and item != '':
                set_.add(item)
                     
    return set_


def ticker_date_dictionary(data, ticker, unique_dates):
    """Pass in a ticker, all the dates in the data, and the data,
    and it will return a dictionary with the dates as a key and
    all the apperances as a value"""
        
    ticker_dict = dict.fromkeys(set(unique_dates), 0)   
    ticker = "/" + ticker + "/"
    
    for date, item in data:
        if ticker in item:
             ticker_dict[date] += 1
    
    return ticker_dict


def resizeTimeframe(data, time_frame = 'minute'):
    time_frame = time_frame.lower()
    temp_lst = []
    
    if time_frame == 'minute' or time_frame == 'm':
        return data
    
    if time_frame == 'hourly' or time_frame == 'h':
        for i in range(len(data)):
            temp_lst.append([data[i][0][:-3], data[i][1]])

        return temp_lst
    
    if time_frame == 'daily' or time_frame == 'd':
        for i in range(len(data)):
            temp_lst.append([data[i][0][:-6], data[i][1]])

        return temp_lst
    

# def data_dataframe(data, time_frame):
#     """Return a dataframe of with columns of all the tickers
#     in the query, and the values for the columns are the 
#     amount of appearance that ticker makes for the given
#     date"""
#     data = resizeTimeframe(data, time_frame)
    
#     query_dates, query_tickers = seperateQuery(data)    

#     unique_tickers = findUniqueTickers(query_tickers)
    
#     df1 = pd.DataFrame(index = set(query_dates))
    
#     for ticker in unique_tickers:
#         df1[ticker] = ticker_date_dictionary(data, ticker, set(query_dates)).values()


#     df1 = df1[sorted(df1.columns)]
#     df1 = df1.sort_index(ascending=True)
    
#     return df1


def data_dataframe(data, time_frame):
    """Return a dataframe of with columns of all the tickers
    in the query, and the values for the columns are the 
    amount of appearance that ticker makes for the given
    date"""
    data = resizeTimeframe(data, time_frame)
    
    query_dates, query_tickers = seperateQuery(data)    

    unique_tickers = findUniqueTickers(query_tickers)
    
    df1 = pd.DataFrame(index = set(query_dates))
    
    for ticker in unique_tickers:
        dict_values = ticker_date_dictionary(data, ticker, set(query_dates))
        temp_df = pd.DataFrame(dict_values.values(), index = dict_values.keys())
        temp_df.rename(columns = {0: ticker}, inplace = True)
        
        df1 = pd.concat((temp_df, df1), axis = 1)
        
        
    df1 = df1[sorted(df1.columns)]
    df1 = df1.sort_index(ascending=True)
    
    return df1


def topValues(dictionary, n = 0):
    return dict(sorted(dictionary.items(), key=lambda x:x[1])[-n:])


def currentDayLeaders(dataframe, n = 0):
    today =  dataframe.index[-1][:-6]
    today_beginning = today + ':07:00'
    today_end = today + ':20:00'

    dataframe = dataframe[today_beginning : today_end]

    leader_dict = dict()
    for ticker in dataframe.columns:
        leader_dict[ticker] = dataframe[ticker].sum()

    return topValues(leader_dict, n)
