from bselib.bse import BSE
import pandas as pd
import sqlite3
import re
import time

conn = sqlite3.connect("database31dec.db")  #initializing a database and a connection
db = conn.cursor()  #initializing a cursor - not required in pandas




b = BSE()
print(b)

#searching a stock and getting the code
stocks = b.script('State Bank of India')
print(stocks)

def script_searcher(name):
    stocks = b.script(name)
    if len(stocks) ==1:
        l1 =list(stocks.keys())
        print(l1[0])
        return(l1[0])
    else:
        print(stocks)
        a=input("choose a stock name from these choose number from 0 to {}".format(len(stocks)-1))
        a = int(a)
        l1 = list(stocks.keys())
        return(l1[a])
        
    
name = "Canara Bank"

sc = script_searcher(name)
print(sc)


#getting a stock quote
data = b.quote('500325')
print(data)
print(data.keys())

#yoy_results, quarter_results, balancesheet and cashflow
fin = b.statement(500325,stats="balancesheet")
print(fin)
print(fin.keys())

name = "Palred"
sc=script_searcher(name)
fin = b.statement(sc,stats="balancesheet")
print(fin)   #this is crap, use historical statementd
print(fin.keys())
fin = b.statement(sc,stats="yoy_results")
print(fin)


stats = b.stmt_analysis(sc,stats="yoy_results")
print(stats)


fin = b.historical_stats(sc,stats="yoy_results")
print(fin)
df = pd.DataFrame(fin)
print(df.head())
print(df)

ratios = b.ratios(int(sc))  #//sometimes works, sometimes doesnt
print(ratios)

#Building our own dashboards

#BSE500 data list 
#df2 = pd.read_csv('bse_smallcap250.csv')
df2 = pd.read_csv("EQ311221.csv")
print(df2.head())

codes = df2['SC_CODE'].tolist()
errors = []

for sc in codes:
    try:

        company = df2[df2["SC_CODE"] == sc]["SC_NAME"].values[0]
        company = re.sub(" ","",company)
        company = company.lower()
        tablename = "s_{}".format(sc)
        print()
        fin = b.historical_stats(sc,stats="yoy_results")
    #    print(fin)
        df = pd.DataFrame(fin)
        print(df.head())
        print(df)   
        df1 = df.T
        new_header = df1.iloc[0] #grab the first row for the header
        df1 = df1[1:] #take the data less the header row
        df1.columns = new_header #set the header row as the df header
        print(df1.columns)
        df1.columns = ['sales', 'expenses', 'operating_profit', 'opm',
           'other_income', 'interest', 'depreciation', 'pbt',
           'tax_pct', 'net_profit', 'eps', 'dividend_payout']
    #    print(df1.loc["TTM","EPS in Rs"])
        print(df1) 
        df1.to_sql("{}{}".format(tablename,"pnl"),conn,if_exists = 'replace')
        
        fin = b.historical_stats(sc,stats="balancesheet")
    #    print(fin)
        df = pd.DataFrame(fin)
        print(df.head())
        print(df)   
        df1 = df.T
        print(df1)
        new_header = df1.iloc[0] #grab the first row for the header
        df1 = df1[1:] #take the data less the header row
        df1.columns = new_header #set the header row as the df header
        print(df1.columns)
        df1.columns = ['share_capital', 'reserves', 'borrowings', 'other_liabilities',
           'total_liabilities', 'fixed_assets', 'cwip', 'investments',
           'other_assets', 'total_assets']
        print(df1) 
    
    #    print(df1.loc["Mar 2020","Total Assets"])
        df1.to_sql("{}{}".format(tablename,"bal_sheet"),conn,if_exists = 'replace')
        df1.to_csv("{}{}".format(tablename,"balsheet"))
        print("{} done".format(company))
        conn.commit()
        time.sleep(1)
    except Exception as e:
        print(e)
        errors.append(company)
        
    

    


    

