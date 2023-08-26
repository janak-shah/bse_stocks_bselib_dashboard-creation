#bse2

#Go through each stock and find out stocks with 
#sales growth > 10% over last 2 years 
#opm > 10% &
#npm > 10% 

#for all these stocks tha tfulfill above conditions print the following
#company_name,price, pe,bvps,sales,opm,npm,and their growth rates

#bal sheet - 'share_capital', 'reserves', 'borrowings', 'other_liabilities',
#           'total_liabilities', 'fixed_assets', 'cwip', 'investments',
#           'other_assets', 'total_assets'

#pnl - 'sales', 'expenses', 'operating_profit', 'opm',
#           'other_income', 'interest', 'depreciation', 'pbt',
#           'tax_pct', 'net_profit', 'eps', 'dividend_payout'


from bselib.bse import BSE
import pandas as pd
import sqlite3
import re
import time

conn = sqlite3.connect('database31dec.db') #initializing a database and a connection
db = conn.cursor() #initializing a cursor


b = BSE()


mega_list = []
#LIST OF CODES  
df = pd.read_csv("EQ311221.csv")
print(df.head())

codes = df["SC_CODE"].tolist()




for sc in codes:
    
    try:
        #print the company name - get from bhavcopy df
        company = df[df["SC_CODE"]==sc]["SC_NAME"].values[0]
        print(company)
    
        
        pnl_tablename = "s_{}pnl".format(sc)
        bal_tablename = "s_{}bal_sheet".format(sc)
    
    #    pnl_tablename = "pnl{}".format(sc)
    #    bal_tablename = "bs{}".format(sc)
    
        #RETRIEVING PNL DATA
        
        pnl_df = pd.read_sql("SELECT * FROM {}".format(pnl_tablename),conn)
        print(pnl_df.head())
        print(pnl_df.columns)
        #RETRIEVING BALANCE SHEET DATA
        bal_df = pd.read_sql("SELECT * FROM {}".format(bal_tablename),conn)
        print(bal_df.tail())    
        print(bal_df.columns)
        
        
        #convert sting to numeric for all columns
        #standard process - do this manually for all columns
    #    pnl_df["sales"] = pd.to_numeric(pnl_df["sales"], errors = 'coerce')
        
        #faster process
        cols = pnl_df.columns.drop('index')
        pnl_df[cols] = pnl_df[cols].apply(pd.to_numeric, errors = 'coerce')
        print(pnl_df.head())
        
        cols = bal_df.columns.drop('index')
        pnl_df[cols] = bal_df[cols].apply(pd.to_numeric, errors = 'coerce')
        print(bal_df.head())
        
        #calculating some requiured data
        
        pnl_df['opm'] = pnl_df['operating_profit']*100/pnl_df['sales']  
        pnl_df['npm'] = pnl_df['net_profit']*100/pnl_df['sales']
        
        #finding out changes in df rows requires shifting of df
        prev_pnl = pnl_df.shift(1)
        print(prev_pnl.head())
    
        pnl_df['chg_sales'] = (pnl_df['sales'] - prev_pnl['sales'])*100/prev_pnl['sales']
    # =============================================================================
    #     pnl_df['chg_opm'] = (pnl_df['opm'] - prev_pnl['opm'])*100/prev_pnl['opm']
    #     pnl_df['chg_npm'] = (pnl_df['npm'] - prev_pnl['opm'])*100/prev_pnl['opm']
    # =============================================================================
        print(pnl_df.tail())
        
        #for price - there are 2 ways to do it 
        #1) to query the bselib api and get latest price
        #2) to select the closing price from the bhavcopy file
        
        data = b.quote('{}'.format(sc))
        latest_price = data['stockPrice']
        print(latest_price)
        fv = data['faceValue']
        
        pnl_rows = pnl_df.shape[0]
        bal_rows = bal_df.shape[0]
        latest_eps = pnl_df.loc[pnl_rows-1,"eps"]
        
        pe = float(latest_price)/float(latest_eps)
        print(pe)
        
        #finding bvps = (share capital + reserves) / no of shares
        
        #no of shares = sharecapital /fv
        latest_sharecap = bal_df.loc[bal_rows-1,"share_capital"]
        latest_reserves = bal_df.loc[bal_rows-1,"reserves"]
        latest_sales = pnl_df.loc[pnl_rows-1,"sales"]
        latest_opm = pnl_df.loc[pnl_rows-1,"opm"]
        latest_npm = pnl_df.loc[pnl_rows-1,"npm"]
        no_of_shares = float(latest_sharecap)/float(fv)
        book_value = latest_sharecap + latest_reserves
        bvps = float(book_value)/float(no_of_shares)
        print(bvps)
        
        
        #checking for conditions
        print(company)
        latest_sales_growth = pnl_df.loc[pnl_rows-1,"chg_sales"]
        latest_sales_growth = float(latest_sales_growth)
        print(latest_sales_growth)
        prev_year_sales_growth = pnl_df.loc[pnl_rows-2,"chg_sales"]
        prev_year_sales_growth = float(prev_year_sales_growth)
        print(prev_year_sales_growth)
        print(latest_npm)
        print(latest_opm)
        
        if (latest_sales_growth > 10.0 and prev_year_sales_growth > 10.0):
            if (float(latest_opm) > 10 and float(latest_npm) > 10):
                print(company)
                list1 = [company,latest_price,pe,bvps,latest_sales,latest_sales_growth, latest_opm, latest_npm]
                mega_list.append(list1)
 
    except Exception as e:
        print(e)
    
    
columns = ["company","ltp","pe","bvps","sales","sales_growth","opm","npm"]
df2 = pd.DataFrame(mega_list, columns = columns)
df2.to_csv("stocks.csv")
