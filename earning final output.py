import pandas as pd
import requests
from IPython.display import display
from concurrent.futures import ThreadPoolExecutor
import warnings
import time
warnings.filterwarnings('ignore')
def earning_date_with_symbol():
    filepath = r'C:\Users\user\Downloads\weekly_stk_earnings.xlsx' # change path of file

    df = pd.read_excel(filepath)
    earn_date_dic={}
    for i in range(len(df)):
        symbol = df.iloc[i, 0]
        # Convert the earning dates to datetime objects
        symbol_earn_date = pd.to_datetime(df.iloc[i, 1:])
        # Format each earning date to YYYYMMDD using apply
        symbol_earn_date_formatted = symbol_earn_date.apply(lambda x: x.strftime('%Y%m%d') if pd.notna(x) else None)
        # Join the formatted earning dates into a single string
        symbol_earn_date_str = '-'.join([str(x) for x in symbol_earn_date_formatted if x is not None])
        earningdate_list = symbol_earn_date_str.split('-')
        earn_date_dic[symbol]=earningdate_list


    return earn_date_dic
def forward4_expdate_dic(earn_date_dic):#from earning date
  url = "http://127.0.0.1:25510/v2/list/expirations"
  outer_list=[]
  for symbol,dates in earn_date_dic.items():
      querystring = {"root":symbol}
      headers = {"Accept": "application/json"}
      response = requests.get(url, headers=headers, params=querystring)
      #print(response.json())
      data=response.json()
      expiry_dates=data['response'] # expiry dates are in list

      for earn_date in dates:
        root_earndate_expirydate4=[]
        greater_dates = list(filter(lambda date: str(date) > earn_date, expiry_dates))

        #print(list(greater_dates))
        sortexpirybyearndate=greater_dates #RED assuming date is sorted
        req_expiry_date=sortexpirybyearndate[0:4]
        root_earndate_expirydate4.append(symbol)
        root_earndate_expirydate4.append(earn_date)
        root_earndate_expirydate4.append(req_expiry_date)
        outer_list.append(root_earndate_expirydate4)
  return outer_list
def start_end_date(root_earndate_expirydate4_lst):
  url = "http://127.0.0.1:25510/v2/list/dates/option/quote"
  symbol_earndate_expirydate_startdate_enddate_lst=[]
  for symbol,earn_date,expiry_date_lst in root_earndate_expirydate4_lst:
              inner_list=[]
              exp_date=expiry_date_lst[0]
              root=symbol
              querystring = {"root":symbol,"exp":str(exp_date)}
              headers = {"Accept": "application/json"}
              response = requests.get(url, headers=headers, params=querystring)
              data=response.json()
              dates=data['response']
              greater_dates = list(filter(lambda date: str(date) > earn_date, dates))
              lesser_dates = list(filter(lambda date: str(date) < earn_date, dates))

              #print(list(greater_dates))
              # previous_dates=dates<earn_date # RED 
              # forward_dates=dates>earn_date   # RED
              if len(lesser_dates)==0:
                 print(f"lesser dates not found for symbol{symbol} expiry date{exp_date} earndate{earn_date} ")
                 start_date=earn_date
              else:
                 start_date=lesser_dates[-1]                 
              if len(greater_dates)==0:
                 print(f"greater dates not found for symbol{symbol} expiry date{exp_date} earndate{earn_date} ")
                 end_date=earn_date 
              else:
                  end_date=greater_dates[0]
                 
              
             
              inner_list.append(symbol)
              inner_list.append(earn_date)
              inner_list.append(expiry_date_lst)
              inner_list.append(start_date)
              inner_list.append(end_date)
              symbol_earndate_expirydate_startdate_enddate_lst.append(inner_list)

  return symbol_earndate_expirydate_startdate_enddate_lst
def bulk_ohlc_data(symbol_earndate_expirydate_startdate_enddate_lst):
  url = "http://127.0.0.1:25510/v2/bulk_hist/option/ohlc"
  rows = []
  for symbol,earn_date,expiry_date_lst,start_date,end_date in symbol_earndate_expirydate_startdate_enddate_lst:
    for exp_date in expiry_date_lst:
      querystring = {"exp":str(exp_date),"start_date":start_date,"end_date":end_date,"root":symbol,"ivl":"900000"}
      headers = {"Accept": "application/json"}
      response = requests.get(url, headers=headers, params=querystring)

      # Check if the response was successful
      if response.status_code == 200:
          # Load JSON data
          data = response.json()
          # Extract ticks and create a list of dictionaries for the DataFrame
          for entry in data['response']:
              for tick in entry['ticks']:
                  row = {
                      'Symbol': symbol,
                      'Earn Date': earn_date,
                      'ms_of_day': tick[0],
                      'open': tick[1],
                      'high': tick[2],
                      'low': tick[3],
                      'close': tick[4],
                      'volume': tick[5],
                      'count': tick[6],
                      'date': tick[7],
                      'contract_root': entry['contract']['root'],
                      'contract_expiration': entry['contract']['expiration'],
                      'contract_strike': entry['contract']['strike'],
                      'contract_right': entry['contract']['right']
                  }
                  rows.append(row)

  # Create a DataFrame
  df = pd.DataFrame(rows)
  #df.to_excel("D:\\Thetadata\\ohlcdatare.xlsx",index=False)

  return df
def greeks_data(ohlc_df,symbol_earndate_expirydatelst_startdate_enddate_lst):
    dataframes = []
    print(symbol_earndate_expirydatelst_startdate_enddate_lst)
  
    symbol,earndate,expirydatelst,startdate,enddate=symbol_earndate_expirydatelst_startdate_enddate_lst[0]
   

    #uniqueexpdates=ohlc_df.contract_expiration.unique()
    for exp_date in expirydatelst:
        uniquesymbolexp_df=ohlc_df[ohlc_df['contract_expiration']==exp_date]

        unique_dates=uniquesymbolexp_df.date.unique()
        for date in unique_dates:
          uniquesymboldate_df=uniquesymbolexp_df[uniquesymbolexp_df['date']==date]
      
          uniqueop=uniquesymboldate_df.contract_right.unique()
          for op in uniqueop:
            uniquesymboldateop_df=uniquesymboldate_df[uniquesymboldate_df['contract_right']==op]

            lst_strikes=uniquesymboldateop_df.contract_strike.unique()
            for strike in lst_strikes:
              url = "http://127.0.0.1:25510/v2/hist/option/greeks"
              querystring = {"root":symbol,"exp":exp_date,"right":op,"strike":strike,"start_date":date,"end_date":date,"ivl":"900000"}
              headers = {"Accept": "application/json"}
              response = requests.get(url, headers=headers, params=querystring)
              if response.status_code == 200:
                # Load JSON data
                data = response.json()          
                # Print the data to inspect its structure
                #print(data)
                # Extract the column names from the header
                columns = data['header']['format']  # This will give you the list of column names
                # Extract the response data
                response_data = data['response']  # This is the list of data points
                
                df = pd.DataFrame(response_data, columns=columns)
                #'ms_of_day','date','contract_root','contract_expiration','contract_strike','contract_right'
                
                df['contract_right'] = op  # Add a column for the option type (C or P)
                df['contract_strike'] = strike  # Add a column for the strike price
                df['contract_expiration'] = exp_date  # Add a column for the expiration date
                df['contract_root'] = symbol  # Add a column for the root symbol
                
                dataframes.append(df)  # Append the DataFrame to the list
    


    final_df = pd.concat(dataframes, ignore_index=True)
    #final_df.to_excel("D:\\Thetadata\\greeksdatare.xlsx",index=False)
    #display(final_df)  # Display the final DataFrame
    return final_df


def run(symbol,earning_date):
        t1=time.perf_counter()
 
        #symbol_earndate_dic=earning_date_with_symbol()
        symbol_earndate_dic={symbol:[earning_date]}
       # print("extracted earndate")
        root_earndate_expirydate4_lst=forward4_expdate_dic(symbol_earndate_dic)
        #print(f"extracted expiry date{root_earndate_expirydate4_lst}")
        symbol_earndate_expirydatelst_startdate_enddate_lst=start_end_date(root_earndate_expirydate4_lst)
        #print(" extracted start and end date")
        #print(f"extracted start end date {symbol_earndate_expirydatelst_startdate_enddate_lst}")
        ohlc_df=bulk_ohlc_data(symbol_earndate_expirydatelst_startdate_enddate_lst)
        #print("extracted ohlc data")
        greek_df=greeks_data(ohlc_df,symbol_earndate_expirydatelst_startdate_enddate_lst)
        #print("extracted greek data")

        main_df=ohlc_df.merge(greek_df,how='left',on=['ms_of_day','date','contract_root','contract_expiration','contract_strike','contract_right'])

            
        
        #display(main_df.head())
        main_df.to_excel(f"D:\\Thetadata\\earning data\\'{symbol}-{earning_date}'.xlsx",index=False)

        print(f"congratulation you got the output for {symbol}-{earning_date}")
        t2=time.perf_counter()
        print(f"Elapsed time={t2-t1}")
#run('AAPL','20240102')   
def notcall():
  with ThreadPoolExecutor(max_workers=10) as executor:
      filepath = r'C:\Users\user\Downloads\weekly_stk_earnings.xlsx' # change path of file

      df = pd.read_excel(filepath)
    # Melt the DataFrame
      df_melted = df.melt(id_vars=['Symbol'], value_name='Earning Date')

    # Select only the necessary columns
      df_final = df_melted[['Symbol', 'Earning Date']]
      df_final['Earning Date'] = pd.to_datetime(df_final['Earning Date']).dt.strftime("%Y%m%d")
      df_final.sort_values(by='Symbol', inplace=True)
      df_final=df_final.head(1)
      future = executor.map(run,df_final['Symbol'],df_final['Earning Date'])

notcall()
        
