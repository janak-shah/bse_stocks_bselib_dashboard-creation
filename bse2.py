from bselib.bse import BSE
import pandas as pd
import sqlite3
import re
import time


conn = sqlite3.connect("database4.db")  #initializing a database and a connection
db = conn.cursor()  #initializing a cursor - not required in pandas


# =============================================================================
# b = BSE()
# print(b)
# 
# 
# =============================================================================
# =============================================================================
# def script_searcher(name):
#     stocks = b.script(name)
#     if len(stocks) ==1:
#         l1 =list(stocks.keys())
#         print(l1[0])
#         return(l1[0])
#     else:
#         print(stocks)
#         a=input("choose a stock name from these choose number from 0 to {}".format(len(stocks)-1))
#         a = int(a)
#         l1 = list(stocks.keys())
#         return(l1[a])
#         
# 
# =============================================================================

#BSE500 data list 
df2 = pd.read_csv("EQ021121.csv")
print(df2.head())

codes = df2["SC_CODE"].to_list()

#latest price df is required
# =============================================================================
# 
# l = []
# db. execute("SELECT name FROM sqlite_master WHERE type='table';")
# for row in db.fetchall():
#     print(row)
#     l.append(row)
#     
# df = pd.DataFrame(l)
# df.to_csv("tablenames.csv")
# =============================================================================
    

#Conditions to be verified
# =============================================================================
# 
# name = "aarti drugs"
# 
# sc=script_searcher(name)
# print(sc)
# =============================================================================


for sc in codes:


        company = df2[df2["SC_CODE"] == sc]["SC_NAME"].values[0]
        company = re.sub(" ","",company)
        company = company.lower()
        tablename = "s_{}".format(sc)

        pnltable = "{}{}".format(tablename,"pnl")
        bal_table = "{}{}".format(tablename,"bal_sheet")
        
        
        pnl_df = pd.read_sql_query("SELECT * FROM {}".format(pnltable),conn)
        cols = pnl_df.columns.drop('index')
        pnl_df[cols] = pnl_df[cols].apply(pd.to_numeric, errors = 'coerce')
        print(pnl_df.tail())
        
        
        
        bal_df = pd.read_sql_query("SELECT * from  {}".format(bal_table), conn)
#        print(bal_df.head())
        print(bal_df.columns)
        cols = bal_df.columns.drop('index')
        pnl_df[cols] = bal_df[cols].apply(pd.to_numeric, errors = 'coerce')
        print(bal_df.tail())        

        pnl_rows = pnl_df.shape[0]

        pnl_df["opm"] = (pnl_df['operating_profit']*100)/pnl_df['sales']
        pnl_df["npm"] = (pnl_df['net_profit']*100)/pnl_df['sales']
        current_price = df2[df2["SC_CODE"] == sc]["CLOSE"].values[0]
        curr_eps = pnl_df.loc[pnl_rows-1,"eps"]
        curr_sales =  pnl_df.loc[pnl_rows-1,"sales"]
        print("Current eps is {}".format(curr_eps))
        print("Current sales is {}".format(curr_sales))
        prev_pnl_df = pnl_df.shift(1)
        pnl_df["opm_chg"] = (pnl_df['opm']-prev_pnl_df["opm"])*100/prev_pnl_df['opm']
        pnl_df["npm_chg"] = (pnl_df['npm']-prev_pnl_df["npm"])*100/prev_pnl_df['npm']
        pnl_df["sales_chg"] = (pnl_df['sales']-prev_pnl_df["sales"])*100/prev_pnl_df['sales']
        pe = current_price/curr_eps
        print(pnl_df.head())
        break
        
        
        
        

#go through each stock and find out stocks with 
#sales growth >10%
#opm growth >10%
#npm growth >10%

#for these stocks print price, p/e,bvps,debtps, sales ,opm and npm and their growths
















