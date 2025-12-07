import pdb
import time
import datetime
import traceback
from Dhan_Tradehull import Tradehull
import pandas as pd
from pprint import pprint
import talib
from rich import print
import pandas_ta as ta
import xlwings as xw
from client_code_storage import client_code
from token_id_storage import token_id
import winsound
import sqn_lib


client_code             = client_code
token_id                = token_id
tsl                     = Tradehull(client_code,token_id)

opening_balance         = tsl.get_balance()
max_risk_for_today      = (1.0*opening_balance)/100*-1



watchlist               = ['MRF','IDBI', 'IDEA', 'NMDC', 'MAHABANK', 'IOB', 'SUZLON', 'NHPC', 'YESBANK', 'IDFCFIRSTB','PGHH','HONAUT','DIXON']
single_order            = {'name':None, 'date':None , 'entry_time': None, 'entry_price': None, 'buy_sell': None, 'qty': None, 'sl': None, 'exit_time': None, 'exit_price': None, 'pnl': None, 'remark': None, 'traded':None}
orderbook               = {}

wb                        = xw.Book('Trade With Dhan.xlsx')
live_Trading              = wb.sheets['Live_Trading']
completed_orders_sheet    = wb.sheets['completed_orders']


reentry                   = "yes"           #"yes/no"
completed_orders          = []

bot_token           = "8549724310:AAHOJhoxbl2NPzHblsi04cRVabjREadq-UU"
receiver_chat_id    = "6193962152"

live_Trading.range("A2:Z300").value           = None
completed_orders_sheet.range("A2:Z300").value = None


for name in watchlist :
    orderbook[name]    = single_order.copy()

# pdb.set_trace()


while True: 
    
    print("starting while Loop \n\n")
    
    current_time       = datetime.datetime.now()
    market_start = datetime.time(9, 15)
    # if current_time < market_start:
    #     print(f"Waiting for market to open", current_time)
    #     time.sleep(1)
    #     continue
    
    live_pnl = tsl.get_live_pnl() 
    max_loss_hit = live_pnl <= max_risk_for_today
    # market_over  = current_time > datetime.time(15, 15)
    
    
    # if max_loss_hit or market_over:
    #     order_details = tsl.cancel_all_orders()
    #     print(f"Market over Closing all trades !! Bye Bye See you Tomorrow" , current_time)
    #     time.sleep(1)
    #     break
    
    all_ltp = tsl.get_ltp_data(names = watchlist)

    for name in watchlist:
    
        ordeerbook_df                        = pd.DataFrame(orderbook).T
        live_Trading.range('A1').value       = ordeerbook_df
        
        completed_orders_df                                = pd.DataFrame(completed_orders)
        completed_orders_sheet.range('A1').value              = completed_orders_df
        
        
        current_time       = datetime.datetime.now()
        print(f"Scanning    {name} current_time")
        
        
        
    
        try:
            chart              = tsl.get_historical_data(tradingsymbol = name,exchange = 'NSE',timeframe="5")
            chart['rsi']       = talib.RSI(chart['close'], timeperiod=14)
            sqn_lib.sqn(df=chart, period=21)
            chart['market_type'] = chart['sqn'].apply(sqn_lib.market_type)
            
            cc                 = chart.iloc[-2] 
            bc1                = cc['rsi'] > 45             # buy entry conditions
            bc2                = orderbook[name]['traded'] is None
            bc3               = cc['market_type'] != "neutral"
            
        except Exception as e:
            print(e)
            continue
        
                            
            
        if bc1 and bc2 and bc3:
            print("buy ", name, "\t")
            
            margin_available = tsl.get_balance()
            margin_required  = cc['close']/4.5
            
            if margin_available < margin_required:
                print(f"Less margin, not taking order : margin_available is {margin_available} and margin_required is {margin_required} for {name}")
                continue
        
            orderbook[name]['name']           = name
            orderbook[name]['date']           = str(current_time.date())
            orderbook[name]['entry_time']     = str(current_time.time())[:8]
            orderbook[name]['max_holding_time']   = datetime.datetime.now() + datetime.timedelta(hours=2)

            orderbook[name]['buy_sell']       = "BUY"
            orderbook[name]['qty']            = 1
            
            try:
                entry_orderid                       = tsl.order_placement(tradingsymbol=name, exchange='NSE', quantity=orderbook[name]['qty'], price=0, trigger_price=0, order_type='MARKET', transaction_type='BUY', trade_type='MIS')
                
                orderbook[name]['entry_orderid']   = entry_orderid
                orderbook[name]['entry_price']     = tsl.get_executed_price(orderid=orderbook[name]['entry_orderid'])
                
                orderbook[name]['tg']              = round(orderbook[name]['entry_price']*1.002, 1)     #1.20
                orderbook[name]['sl']              = round(orderbook[name]['entry_price']*0.998, 1)     #0.90
                sl_orderid                          = tsl.order_placement(tradingsymbol=name, exchange='NSE', quantity=orderbook[name]['qty'], price=0, trigger_price=orderbook[name]['sl'], order_type='STOPMARKET', transaction_type='SELL', trade_type='MIS')
                orderbook[name]['sl_orderid']      = sl_orderid
                orderbook[name]['traded']          = "yes"
                
                message = "\n".join(f"'{key}': {repr(value)}" for key, value in orderbook[name].items())
                message = f"Entry_done {name} \n\n {message}"
                tsl.send_telegram_alert(message=message,receiver_chat_id=receiver_chat_id,bot_token=bot_token)
                
                
            except Exception as e:
                print(e)
                # pdb.set_trace(header="error in placing entry order")
        
        
        
        
        if orderbook[name]['traded'] == "yes":
            bought = orderbook[name]['buy_sell'] == "BUY"
        
            
            if bought:
                
                try:
                    ltp     = all_ltp[name]
                    sl_hit = tsl.get_order_status(orderid=orderbook[name]['sl_orderid']) == "TRADED"
                    tg_hit = ltp > orderbook[name]['tg']
                    
                    
                    max_holding_time_exceeded = datetime.datetime.now() > orderbook[name]['max_holding_time']
                    current_pnl           = round((ltp - orderbook[name]['entry_price'])*orderbook[name]['qty'],1)
                    
                    
                except Exception as e:
                    print(e)
                    pdb.set_trace(header="error in checking sl/tg hit")    
                    pass
                
                
                
                
                if sl_hit:            
                    
                    try:                     
                      orderbook[name]['exit_time']  = str(current_time.time())[:8]
                      orderbook[name]['exit_price'] = tsl.get_executed_price(orderid=orderbook[name]['sl_orderid'])
                      orderbook[name]['pnl']        = round((orderbook[name]['exit_price'] - orderbook[name]['entry_price'])*orderbook[name]['qty'],1)
                      orderbook[name]['remark']     = "Bought_SL_hit"
                      
                      message = "\n".join(f"'{key}': {repr(value)}" for key, value in orderbook[name].items())
                      message = f"SL_hit {name} \n\n {message}"
                      tsl.send_telegram_alert(message=message,receiver_chat_id=receiver_chat_id,bot_token=bot_token)
                      
                    
                      if reentry == "yes":
                        completed_orders.append(orderbook[name])
                        orderbook[name] = single_order.copy()
                        
                    except Exception as e:
                        print(e)
                        pdb.set_trace(header="error in sl_hit")
                        pass

                
                if tg_hit:
                    
                    try:
                        
                        tsl.cancel_order(OrderID=orderbook[name]['sl_orderid'])
                        time.sleep(2)
                        square_off_buy_order          = tsl.order_placement(tradingsymbol=orderbook[name]['name'] ,exchange='NSE', quantity=orderbook[name]['qty'], price=0, trigger_price=0,    order_type='MARKET',     transaction_type='SELL',   trade_type='MIS')
                        
                        orderbook[name]['exit_time']  = str(current_time.time())[:8]
                        orderbook[name]['exit_price'] = tsl.get_executed_price(orderid=square_off_buy_order)
                        orderbook[name]['pnl']        = (orderbook[name]['exit_price'] - orderbook[name]['entry_price'])*orderbook[name]['qty']
                        orderbook[name]['remark']     = "Bought_TG_hit"
                        
                        message = "\n".join(f"'{key}': {repr(value)}" for key, value in orderbook[name].items())
                        message = f"TG_hit {name} \n\n {message}"
                        tsl.send_telegram_alert(message=message,receiver_chat_id=receiver_chat_id,bot_token=bot_token)
                    
                        if reentry == "yes":
                            completed_orders.append(orderbook[name])
                            orderbook[name] = single_order.copy()
                            
                    except Exception as e:
                        print(e)
                        pdb.set_trace(header="error in tg_hit")
                        pass
                        
                        
                        

                    if max_holding_time_exceeded and (current_pnl < 0):

                        try:
                            tsl.cancel_order(OrderID=orderbook[name]['sl_orderid'])
                            time.sleep(2)
                            square_off_buy_order          = tsl.order_placement(tradingsymbol=orderbook[name]['name'] ,exchange='NSE', quantity=orderbook[name]['qty'], price=0, trigger_price=0,    order_type='MARKET',     transaction_type='SELL',   trade_type='MIS')

                            orderbook[name]['exit_time']  = str(current_time.time())[:8]
                            orderbook[name]['exit_price'] = tsl.get_executed_price(orderid=square_off_buy_order)
                            orderbook[name]['pnl']        = (orderbook[name]['exit_price'] - orderbook[name]['entry_price'])*orderbook[name]['qty']
                            orderbook[name]['remark']     = "holding_time_exceeded_and_I_am_still_facing_loss"

                            message = "\n".join(f"'{key}': {repr(value)}" for key, value in orderbook[name].items())
                            message = f"holding_time_exceeded_and_I_am_still_facing_loss {name} \n\n {message}"
                            tsl.send_telegram_alert(message=message,receiver_chat_id=receiver_chat_id,bot_token=bot_token)

                            if reentry == "yes":
                                completed_orders.append(orderbook[name])
                                orderbook[name] = single_order.copy()

                        except Exception as e:
                            print(e)
                            pdb.set_trace(header="error in holding_time_exceeded")
                            pass
