
from breeze_connect import BreezeConnect
from pandas.core.frame import com
import streamlit as st
import pandas as pd
import dotenv
import os
from collections import namedtuple
import urllib
from util import penny
import datetime

def get_julian_date():
    now = datetime.datetime.now()
    julian_date = now.strftime('%Y%j')  # Format: YYYYDDD
    return julian_date

dotenv.load_dotenv()

api_key = os.getenv('api_key')
api_secret = os.getenv('secret_key')
session_token = os.getenv('session_token')


print("https://api.icicidirect.com/apiuser/login?api_key="+urllib.parse.quote_plus(api_key))

breeze = BreezeConnect(api_key)

breeze.generate_session(api_secret,session_token)

company_stock_dict ={}
isec_code = []


print('getting')

for each_stock in penny:
    local_var = breeze.get_names(
        exchange_code='NSE',
        stock_code=each_stock
    )
    isec_code.append(local_var['isec_stock_code'])
    company_stock_dict[local_var['company name']] = each_stock

print(f'Loaded {len(isec_code)} stocks')


st.dataframe(company_stock_dict)

breeze.ws_connect()


QuoteData = namedtuple('QuoteData', [
    'open', 'close', 'high', 'low', 'last', 'change', 'buy_price', 'buy_qty',
    'sell_price', 'sell_qty', 'ltq', 'avg_price', 'ttq', 'total_buyQ', 'total_sellQ',
    'ttv', 'lowerCktLm', 'upperCktLm', 'time', 'stock_code'
])

MarketData = namedtuple('MarketData', [
    'time', 'stock_code',
    'best_buy_rate', 'best_buy_qty', 'buy_order',
    'best_sell_rate', 'best_sell_qty', 'sell_order'
])

qData = []
mData = []

def process_quotes_data(ticks):
    return QuoteData(
        open=ticks['open'],
        close=ticks['close'],
        high=ticks['high'],
        low=ticks['low'],
        last=ticks['last'],
        change=ticks['change'],
        buy_price=ticks['bPrice'],
        buy_qty=ticks['bQty'],
        sell_price=ticks['sPrice'],
        sell_qty=ticks['sQty'],
        ltq=ticks['ltq'],
        avg_price=ticks['avgPrice'],
        ttq=ticks['ttq'],
        total_buyQ=ticks['totalBuyQt'],
        total_sellQ=ticks['totalSellQ'],
        ttv=ticks['ttv'],
        lowerCktLm=ticks['lowerCktLm'],
        upperCktLm=ticks['upperCktLm'],
        time=ticks['ltt'],
        stock_code=company_stock_dict.get(ticks['stock_name'], '')
    )

def process_market_data(ticks):
    total_best_buy_rate = 0
    total_buy_qty = 0
    total_best_sell_rate = 0
    total_sell_qty = 0
    sell_order = 0
    buy_order = 0
    depth_data = ticks['depth']
    
    for depth in depth_data:
        for key, value in depth.items():
            if 'BestBuyRate' in key:
                total_best_buy_rate += value
            elif 'BestBuyQty' in key:
                total_buy_qty += value
            elif 'BestSellRate' in key:
                total_best_sell_rate += value
            elif 'BestSellQty' in key:
                total_sell_qty += value
            elif 'SellNoOfOrders' in key:
                sell_order += value
            elif 'BuyNoOfOrders' in key:
                buy_order += value

    avg_best_buy_rate = round(total_best_buy_rate / 5, 2)
    avg_best_sell_rate = round(total_best_sell_rate / 5, 2)
    
    return MarketData(
        time=ticks['time'],
        stock_code=company_stock_dict.get(ticks['stock_name'], ''),
        best_buy_rate=avg_best_buy_rate,
        best_buy_qty=total_buy_qty,
        buy_order=buy_order,
        best_sell_rate=avg_best_sell_rate,
        best_sell_qty=total_sell_qty,
        sell_order=sell_order
    )

def on_ticks2(ticks):
    global qData, mData
    
    if ticks['quotes'] == 'Quotes Data':
        qData.append(process_quotes_data(ticks))
        print(process_quotes_data(ticks))
    else:
        mData.append(process_market_data(ticks))
        print(process_market_data(ticks))
    
    # Periodically save to CSV to avoid excessive I/O
    julian_date = get_julian_date()
    if len(qData) >= 100:
        qDF = pd.DataFrame(qData)
        qDF.to_csv(f'qData{julian_date}.csv', mode='a', header=not pd.io.common.file_exists('qData.csv'), index=False)
        qData = []
        
    if len(mData) >= 100:
        mDF = pd.DataFrame(mData)
        mDF.to_csv(f'mData{julian_date}.csv', mode='a', header=not pd.io.common.file_exists('mData.csv'), index=False)
        mData = []

breeze.on_ticks2 = on_ticks2



for member in isec_code:
        breeze.subscribe_feeds(
        exchange_code='NSE',    
        stock_code= member,
        product_type='cash',
             get_exchange_quotes=True,
             get_market_depth=True,
    )

