from bselib.bse import BSE
import pandas as pd
import sqlite3
import re
import time

conn = sqlite3.connect("database5.db") #initializing a database and a connection


b = BSE()
print(b)

# =============================================================================
# #search any stock and obtain its code
# stocks = b.script('reliance')
# print(stocks)  #dictionary 
# =============================================================================

# =============================================================================
# 
# #top performers
# top_performers = b.get_gainers()
# print(top_performers)   #list of dictionaries
# 
# 
# =============================================================================

#checking the integrity of the api

# =============================================================================
# v= b.statement(532521, stats = 'yoy_results')
# print(v)
# 
# 
# v = b.historical_stats(532521, stats='yoy_results')
# print(v)
# =============================================================================

#searching a stock name and getting the code

#name = input("enter a stock whose code you need")

def script_searcher(b,name):
    stocks = b.script(name)
    if len(stocks) ==1:
        #making a list of keys
        l1 = list(stocks.keys())
        print(l1[0])
        return(l1[0])
    else:
        print(stocks)
        c = len(stocks)
        a = input("choose a stock name from these and choose its number from 0 to {}".format(c-1))
        a = int(a)
        l1 = list(stocks.keys())
        print(l1[a])
        return(l1[a])
        
        
        
#code = script_searcher(b,name)



# =============================================================================
# v = b.historical_stats(code, stats='yoy_results')
# print(v)
# 
# #CLEANING P&L DATA
# 
# 
# df = pd.DataFrame(v)
# print(df.head())
# print(df.columns)
# #find transpose of this dataframe
# df1 = df.T 
# print(df1)
# print(df1.columns)
# #nneed to set first row of the data as header
# new_header = df1.iloc[0]  #saves the entire first row in thi variable 
# print(new_header)
# df1 = df1[1:]   #it will ignore the first row and store all the other rows in variable df1
# df1.columns = new_header  #set the header of the dataframe
# print(df1.columns)
# print(df1.head())
# 
# 
# df1.columns = ['sales', 'expenses', 'operating_profit', 'opm_pct', 'other_income',
#        'interest', 'depreciation', 'pbt', 'tax_pct', 'net_profit',
#        'eps', 'div_payout_pct']
# 
# print(df1.columns)
# print(df1.head())
# 
# 
# #if i want march 2021 eps
# print(df1.loc["Mar 2021","eps"])
# 
# 
# #CLEANING BALANCE SHEET DATA
# 
# v = b.historical_stats(code, stats='balancesheet')
# print(v)
# 
# 
# 
# df = pd.DataFrame(v)
# print(df.head())
# print(df.columns)
# #find transpose of this dataframe
# df1 = df.T 
# print(df1)
# print(df1.columns)
# #nneed to set first row of the data as header
# new_header = df1.iloc[0]  #saves the entire first row in thi variable 
# print(new_header)
# df1 = df1[1:]   #it will ignore the first row and store all the other rows in variable df1
# df1.columns = new_header  #set the header of the dataframe
# print(df1.columns)
# print(df1.head())
# 
# df1.columns = ['share_capital', 'reserves', 'borrowings', 'other_liabilities',
#        'total_liabilities', 'fixed_assets', 'cwip', 'investments',
#        'other_assets', 'total_assets']
# 
# 
# print(df1.columns)
# print(df1.head())
# =============================================================================

#storing p&l and balancesheet data of all stocks in sqlite3 database 

error_stocks = []
df2 = pd.read_csv("EQ021121.csv")

#LOOPING THROUGH ALL STOCKS OF BSE BHAVCOPY 

for code in df2["SC_CODE"]:
    
    try:
        company_name = df2[df2["SC_CODE"]==code]["SC_NAME"]
        print(company_name)
        v = b.historical_stats(code, stats='yoy_results')
    
        
        #CLEANING P&L DATA
        
        
        df = pd.DataFrame(v)
    
        #find transpose of this dataframe
        df1 = df.T 
    
        #nneed to set first row of the data as header
        new_header = df1.iloc[0]  #saves the entire first row in thi variable 
    
        df1 = df1[1:]   #it will ignore the first row and store all the other rows in variable df1
        df1.columns = new_header  #set the header of the dataframe
        
        
        df1.columns = ['sales', 'expenses', 'operating_profit', 'opm_pct', 'other_income',
               'interest', 'depreciation', 'pbt', 'tax_pct', 'net_profit',
               'eps', 'div_payout_pct']
        
        print(df1.columns)
        print(df1.head())
        
        df1.to_sql("pnl{}".format(code),conn,if_exists ='replace')
        
        #CLEANING BALANCE SHEET DATA
        
        v = b.historical_stats(code, stats='balancesheet')
        
        
        
        df = pd.DataFrame(v)
    
        #find transpose of this dataframe
        df1 = df.T 
    
        #nneed to set first row of the data as header
        new_header = df1.iloc[0]  #saves the entire first row in thi variable 
    
        df1 = df1[1:]   #it will ignore the first row and store all the other rows in variable df1
        df1.columns = new_header  #set the header of the dataframe
    
        
        df1.columns = ['share_capital', 'reserves', 'borrowings', 'other_liabilities',
               'total_liabilities', 'fixed_assets', 'cwip', 'investments',
               'other_assets', 'total_assets']
        
        
        print(df1.columns)
        print(df1.head())
        df1.to_sql("bs{}".format(code),conn,if_exists = 'replace')
        conn.commit()
    
        print("{} DONE".format(company_name))
        time.sleep(1)  #introduce a delay of 1 second between stocks 
        
    except Exception as e:
        print(e)
        error_stocks.append(company_name)
        
        
print(error_stocks)
    
    
#try and except loop - for handling of errors





        
