import pandas as pd
import requests
from IPython.display import display
from concurrent.futures import ThreadPoolExecutor
import warnings
import time
warnings.filterwarnings('ignore')


def convert_ms_to_time(ms_of_day):
  """Converts milliseconds of the day to HH:MM:SS format.

  Args:
    ms_of_day: Milliseconds of the day.

  Returns:
    A string representing the time in HH:MM:SS format.
  """
  seconds = ms_of_day // 1000
  minutes = seconds // 60
  hours = minutes // 60
  seconds %= 60  #seconds = seconds%60 
  minutes %= 60

  return "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds)


def forward4_expdate_dic(symbol,earn_date):
  '''
  It will take arg symbol and eaning data

  return
  immediate 4 expiry dates from earning date
  '''

  url = "http://127.0.0.1:25510/v2/list/expirations"
  querystring = {"root":symbol}
  headers = {"Accept": "application/json"}
  response = requests.get(url, headers=headers, params=querystring)
  #print(response.json())
  data=response.json()
  expiry_dates=data['response'] # expiry dates are in list 
  greater_4exp_dates = list(filter(lambda date: str(date) > earn_date, expiry_dates))
  
  return greater_4exp_dates[0:4]


def start_end_date(symbol,earn_date,greater_4exp_dates):
  '''
  It will take symbol ,(4 expiry date immediate to earning date) and earning date as arg

  return 

  immediate back date from  earn date and next immediate date as start and end date respectively. 
  '''
  exp_date=greater_4exp_dates[0] # immediate expiry date
  url = "http://127.0.0.1:25510/v2/list/dates/option/quote"             
  querystring = {"root":symbol,"exp":str(exp_date)}
  headers = {"Accept": "application/json"}
  response = requests.get(url, headers=headers, params=querystring)
  data=response.json()
  dates=data['response']
  greater_dates = list(filter(lambda date: str(date) > earn_date, dates))
  lesser_dates = list(filter(lambda date: str(date) < earn_date, dates))

  if len(lesser_dates)==0:
      print(f"lesser dates not found for symbol{symbol} expiry date{exp_date} earndate{earn_date} ")
      return "no data found" 
  else:
      start_date=lesser_dates[-1]                 
  if len(greater_dates)==0:
      print(f"greater dates not found for symbol{symbol} expiry date{exp_date} earndate{earn_date}")
      return "no data found"

  else:
      end_date=greater_dates[0]
       
  return start_date,end_date


def bulk_ohlc_data(symbol,earn_date,greater_4exp_dates,start_date,end_date):
  url = "http://127.0.0.1:25510/v2/bulk_hist/option/ohlc"
  rows = []
  
  for exp_date in greater_4exp_dates:
      querystring = {"exp":str(exp_date),"start_date":start_date,"end_date":end_date,"root":symbol,"ivl":"900000"}
      headers = {"Accept": "application/json"}
      response = requests.get(url, headers=headers, params=querystring)

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
  ohlc_df = pd.DataFrame(rows)
  #df.to_excel("D:\\Thetadata\\ohlcdatare.xlsx",index=False)
  return ohlc_df


def greeks_data(ohlc_df,symbol, earn_date, greater_4exp_dates,startdate,enddate):
    dataframes = []
    
    for exp_date in greater_4exp_dates:
        exp_df=ohlc_df[ohlc_df['contract_expiration']==exp_date]

        unique_dates=exp_df.date.unique()
        for date in unique_dates:
          date_df=exp_df[exp_df['date']==date]
      
          uniqueop=date_df.contract_right.unique()
          for op in uniqueop:
            op_df=date_df[date_df['contract_right']==op]

            lst_strikes=op_df.contract_strike.unique()
            for strike in lst_strikes:
              url = "http://127.0.0.1:25510/v2/hist/option/greeks"
              querystring = {"root":symbol,"exp":exp_date,"right":op,"strike":strike,"start_date":date,"end_date":date,"ivl":"900000"}
              headers = {"Accept": "application/json"}
              response = requests.get(url, headers=headers, params=querystring)
              
              data = response.json()          
                
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
    


    greeks_df = pd.concat(dataframes, ignore_index=True)
    #final_df.to_excel("D:\\Thetadata\\greeksdatare.xlsx",index=False)
    #display(final_df)  # Display the final DataFrame
    return greeks_df


def run(symbol,earning_date):
        print(f"symbol {symbol} and earn_date {earning_date}")
      
        t1=time.perf_counter() # timer start

        greater_4exp_dates=forward4_expdate_dic(symbol,earning_date)# calculating 4 forward expiry dates
        print(f"expiry dates {greater_4exp_dates}")

        result=start_end_date(symbol,earning_date,greater_4exp_dates)               
        if result == "no data found":
          print("Not processing  this case ")
        else:
            start_date,end_date=result
            print(f'start and end date is {result}')
                      
            ohlc_df=bulk_ohlc_data(symbol,earning_date,greater_4exp_dates,start_date,end_date)
          

            greek_df=greeks_data(ohlc_df,symbol, earning_date, greater_4exp_dates,start_date,end_date)
            

            main_df=ohlc_df.merge(greek_df,how='left',on=['ms_of_day','date','contract_root','contract_expiration','contract_strike','contract_right'])
            main_df['time']=main_df['ms_of_day'].apply(convert_ms_to_time)
            
            #display(main_df.head())
            main_df.to_excel(f"D:\\Thetadata\\earning data\\'{symbol}-{earning_date}'.xlsx",index=False)
            #print(f"data converted into excel")

        print(f"congratulation you got the output for {symbol}-{earning_date}")
        t2=time.perf_counter()
        print(f"Elapsed time={(t2-t1)/60}")
#run('AAPL','20240102')   
def call_thread():
  with ThreadPoolExecutor(max_workers=500) as executor:
      filepath = r'D:\Crewhub data\US stocks earning dates quaterly\df_0.xlsx' # change path of file
      df = pd.read_excel(filepath)     
      # df['Earning Date']=pd.to_datetime(df['Earning Date'],format='%d-%m-%Y').dt.strftime(%Y%m%d)  
      future = executor.map(run,df['Symbol'],df['Earnings_Date']) # Earning Date

      

if __name__ == '__main__':
  start = time.perf_counter()
  run('ACB','20240208')
  #call_thread()
  end = time.perf_counter()
  print(f"\nTotal elapsed Time df: {round((end-start)/60,2)} min")   
        
