import pdb
import time
import datetime
import traceback
from Dhan_Tradehull import Tradehull
import pandas as pd
from pprint import pprint
import talib
import pandas_ta as ta
import xlwings as xw
from client_code_storage import client_code
from token_id_storage import token_id
import winsound
import sqn_lib

client_code = client_code
token_id = token_id
print("="*70)
print("🚀 DHAN MCX TRADING BOT - GOLD FEB Scanner with GOLDPETAL Trading")
print("="*70)

# === VALIDATE TOKEN ===
print("\n🔐 Testing Dhan API authentication...")
try:
    tsl = Tradehull(client_code, token_id)
    test_balance = tsl.get_balance()
    if isinstance(test_balance, dict) and test_balance.get('status') == 'failure':
        error_code = test_balance.get('remarks', {}).get('error_code', 'Unknown')
        error_msg = test_balance.get('remarks', {}).get('error_message', 'Unknown error')
        print(f"\n❌ AUTHENTICATION FAILED!")
        print(f"   Error Code: {error_code}")
        print(f"   Error: {error_msg}")
        print(f"\n📝 FIX: Regenerate token from Dhan Web")
        exit()
    opening_balance = float(test_balance)
    print(f"✅ Connected! Balance: ₹{opening_balance:,.2f}\n")
except Exception as e:
    print(f"\n❌ Connection Error: {e}")
    exit()

# === CONFIGURATION ===
max_risk_for_today = (1.0*opening_balance)/100*-1
SCAN_SYMBOL_OPTIONS = ['GOLD FEB FUT', 'GOLD05FEB26FUT', 'GOLDFEB26FUT', 'GOLD']
TRADING_SYMBOL_OPTIONS = ['GOLDPETAL JAN FUT', 'GOLDPETAL31JAN26FUT', 'GOLDPETAL', 'GOLDPETAL DEC FUT', 'GOLDPETAL31DEC25FUT']

# Test GOLD FEB
print("🔍 Testing GOLD FEB scan symbol formats...")
scan_symbol = None
for symbol in SCAN_SYMBOL_OPTIONS:
    try:
        test_gold = tsl.get_ltp_data(names=[symbol])
        if not isinstance(test_gold, dict) or test_gold.get('status') != 'failure':
            scan_symbol = symbol
            print(f"✅ Valid GOLD scan symbol found: {symbol}")
            break
        else:
            print(f"⚠️ Symbol not found: {symbol}")
    except:
        continue

if not scan_symbol:
    print(f"❌ GOLD FEB symbol not found!")
    exit()

# Test GOLDPETAL
print("\n🔍 Testing GOLDPETAL trading symbol formats...")
trading_symbol = None
for symbol in TRADING_SYMBOL_OPTIONS:
    try:
        test_data = tsl.get_ltp_data(names=[symbol])
        if not isinstance(test_data, dict) or test_data.get('status') != 'failure':
            trading_symbol = symbol
            print(f"✅ Valid trading symbol found: {symbol}")
            break
        else:
            print(f"⚠️ Symbol not found: {symbol}")
    except:
        continue

if not trading_symbol:
    print("\n❌ Could not find valid GOLDPETAL contract!")
    exit()

watchlist = [scan_symbol]
print(f"\n{'='*70}")
print(f"📊 SCAN Symbol: {scan_symbol}")
print(f"💰 TRADE Symbol: {trading_symbol}")
print(f"{'='*70}")

single_order = {'name':None, 'date':None, 'entry_time': None, 'entry_price': None,
                'buy_sell': None, 'qty': None, 'sl': None, 'exit_time': None,
                'exit_price': None, 'pnl': None, 'remark': None, 'traded':None}

orderbook = {}
wb = xw.Book('Gold_Trade_Data.xlsx')
live_Trading = wb.sheets['Live_Trading']
completed_orders_sheet = wb.sheets['completed_orders']
reentry = "yes"
completed_orders = []
bot_token = "8549724310:AAHOJhoxbl2NPzHblsi04cRVabjREadq-UU"
receiver_chat_id = "6193962152"
receiver_chat_id_2 = "1169187573"
live_Trading.range("A2:Z300").value = None
completed_orders_sheet.range("A2:Z300").value = None
orderbook[trading_symbol] = single_order.copy()
consecutive_api_failures = 0
MAX_API_FAILURES = 3

print(f"💰 Max Loss: ₹{max_risk_for_today:,.2f}")
print(f"⏰ Started: {datetime.datetime.now()}\n")

def check_token_validity(response, api_name):
    if isinstance(response, dict) and response.get('status') == 'failure':
        remarks = response.get('remarks', {})
        error_code = remarks.get('error_code', '')
        error_msg = remarks.get('error_message', remarks.get('message', ''))
        if 'DH-906' in str(error_code) or 'Invalid Token' in str(error_msg) or 'DH-901' in str(error_code):
            print(f"\n{'='*70}")
            print(f"🔴 TOKEN EXPIRED - {api_name} Failed")
            print(f"{'='*70}\n")
            try:
                tsl.send_telegram_alert(message=f"🔴 BOT STOPPED - Token Expired\nAPI: {api_name}", receiver_chat_id=receiver_chat_id, bot_token=bot_token)
                tsl.send_telegram_alert(message=f"🔴 BOT STOPPED - Token Expired\nAPI: {api_name}", receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)
            except:
                pass
            return True
    return False

while True:
    print("🔄 Starting scan...")
    current_time = datetime.datetime.now()
    
    try:
        live_pnl = tsl.get_live_pnl()
        if check_token_validity(live_pnl, "Live PNL"):
            consecutive_api_failures += 1
            if consecutive_api_failures >= MAX_API_FAILURES:
                break
            time.sleep(10)
            continue
        consecutive_api_failures = 0
        print(f"✅ Live PNL: ₹{live_pnl:,.2f}")
    except Exception as e:
        consecutive_api_failures += 1
        print(f"❌ PNL Error: {e}")
        if consecutive_api_failures >= MAX_API_FAILURES:
            break
        time.sleep(10)
        continue
    
    if live_pnl <= max_risk_for_today:
        print(f"🛑 MAX LOSS HIT!")
        break
    
    # GET LTP with VALIDATION
    try:
        all_ltp = tsl.get_ltp_data(names=[scan_symbol, trading_symbol])
        if check_token_validity(all_ltp, "LTP"):
            time.sleep(10)
            continue
        
        # VALIDATE LTP DATA - FIXED VERSION
        if isinstance(all_ltp, dict) and all_ltp.get('status') == 'failure':
            print(f"⚠️ LTP API failed: {all_ltp}")
            time.sleep(10)
            continue
        
        # Check if both symbols have LTP data
        missing_symbols = []
        if scan_symbol not in all_ltp:
            missing_symbols.append(scan_symbol)
        if trading_symbol not in all_ltp:
            missing_symbols.append(trading_symbol)
        
        if missing_symbols:
            print(f"⚠️ LTP unavailable for: {', '.join(missing_symbols)}")
            print(f"   Available symbols: {list(all_ltp.keys())}")
            time.sleep(10)
            continue
    except Exception as e:
        print(f"❌ LTP Error: {e}")
        time.sleep(10)
        continue
    
    for scan_name in watchlist:
        ordeerbook_df = pd.DataFrame(orderbook).T
        live_Trading.range('A1').value = ordeerbook_df
        completed_orders_df = pd.DataFrame(completed_orders)
        completed_orders_sheet.range('A1').value = completed_orders_df
        
        print(f"\n🔍 Scanning {scan_name} at {current_time.strftime('%H:%M:%S')}")
        
        try:
            chart = tsl.get_historical_data(tradingsymbol=scan_name, exchange='MCX', timeframe="15")
            if check_token_validity(chart, "Historical Data"):
                continue
            
            if chart is None or len(chart) < 26:
                print(f"⚠️ Insufficient data")
                continue
            
            chart['rsi'] = talib.RSI(chart['close'], timeperiod=14)
            chart['MACD'], chart['MACD_Signal'], chart['MACD_Hist'] = talib.MACD(chart['close'], fastperiod=12, slowperiod=26, signalperiod=9)
            sqn_lib.sqn(df=chart, period=21)
            chart['market_type'] = chart['sqn'].apply(sqn_lib.market_type)
            
            cc = chart.iloc[-2]
            bc1 = cc['rsi'] > 65
            bc2 = orderbook[trading_symbol]['traded'] is None
            bc3 = cc['market_type'] != "neutral"
            bc4 = cc['MACD'] > cc['MACD_Signal']
            
            # MACD Zone Detection
            macd_value = cc['MACD']
            macd_signal = cc['MACD_Signal']
            macd_diff = macd_value - macd_signal
            
            # Determine zone
            if macd_value > 0:
                macd_zone = "Positive"
                zone_color = "[green]"
            else:
                macd_zone = "Negative"
                zone_color = "[red]"
            
            # Check for crossover (comparing with previous candle)
            prev_cc = chart.iloc[-3]
            prev_macd_diff = prev_cc['MACD'] - prev_cc['MACD_Signal']
            
            if macd_diff > 0 and prev_macd_diff <= 0:
                macd_status = "Bullish Crossover"
                status_color = "[green]"
            elif macd_diff < 0 and prev_macd_diff >= 0:
                macd_status = "Bearish Crossover"
                status_color = "[red]"
            elif macd_diff > 0:
                macd_status = "Above Signal"
                status_color = "[green]"
            else:
                macd_status = "Below Signal"
                status_color = "[red]"
            
            # Market type color
            if cc['market_type'] == "bullish":
                market_color = "[green]"
            elif cc['market_type'] == "bearish":
                market_color = "[red]"
            else:
                market_color = "[yellow]"
            
            print(f" 📊 RSI={cc['rsi']:.2f} | Market={market_color}{cc['market_type']}[/{market_color.strip('[')}]")
            print(f" 📈 MACD={macd_value:.2f} | Signal={macd_signal:.2f} | [yellow]Zone=[/yellow]{zone_color}{macd_zone}[/{zone_color.strip('[')}] | [yellow]Status=[/yellow]{status_color}{macd_status}[/{status_color.strip('[')}]")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            traceback.print_exc()
            continue
        
        if bc1 and bc2 and bc3 and bc4:
            print(f"🎯 BUY SIGNAL FROM {scan_name}!")
            print(f"📈 Placing order in {trading_symbol}...")
            
            try:
                # SAFE LTP ACCESS
                if trading_symbol not in all_ltp:
                    print(f"❌ Cannot get {trading_symbol} price")
                    continue
                
                trading_ltp = all_ltp[trading_symbol]
                if not isinstance(trading_ltp, (int, float)):
                    print(f"❌ Invalid price: {trading_ltp}")
                    continue
                
                # MARGIN CHECK - FIXED VERSION
                margin_available = tsl.get_balance()
                margin_required = trading_ltp/8                     #4.5
                if margin_available < margin_required:
                    shortfall = margin_required - margin_available
                    print(f"⚠️ LOW MARGIN!")
                    print(f"   💰 Available: ₹{margin_available:,.2f}")
                    print(f"   📊 Required: ₹{margin_required:,.2f}")
                    print(f"   ❌ Shortfall: ₹{shortfall:,.2f}")
                    print(f"   💡 Add ₹{shortfall:,.2f} to place this order")
                    continue
                
                orderbook[trading_symbol]['name'] = trading_symbol
                orderbook[trading_symbol]['date'] = str(current_time.date())
                orderbook[trading_symbol]['entry_time'] = str(current_time.time())[:8]
                orderbook[trading_symbol]['max_holding_time'] = datetime.datetime.now() + datetime.timedelta(hours=2)
                orderbook[trading_symbol]['buy_sell'] = "BUY"
                orderbook[trading_symbol]['qty'] = 1
                
                # PLACE ENTRY ORDER WITH RETRY LOGIC
                entry_orderid = None
                for attempt in range(3):
                    try:
                        print(f"📤 Placing BUY order for {trading_symbol} (Attempt {attempt+1}/3)...")
                        entry_orderid = tsl.order_placement(
                            tradingsymbol=trading_symbol,
                            exchange='MCX',
                            quantity=1,
                            price=0,
                            trigger_price=0,
                            order_type='MARKET',
                            transaction_type='BUY',
                            trade_type='MIS'
                        )
                        
                        if entry_orderid and not isinstance(entry_orderid, dict):
                            print(f"✅ Entry Order ID: {entry_orderid}")
                            break
                        else:
                            print(f"⚠️ Invalid response: {entry_orderid}")
                            time.sleep(2)
                    except Exception as e:
                        print(f"⚠️ Order attempt failed: {e}")
                        if attempt < 2:
                            time.sleep(2)
                
                if not entry_orderid or isinstance(entry_orderid, dict):
                    print(f"❌ Entry order failed after 3 attempts!")
                    continue
                
                orderbook[trading_symbol]['entry_orderid'] = entry_orderid
                
                # WAIT AND GET EXECUTED PRICE WITH RETRY
                entry_price = None
                for attempt in range(5):
                    time.sleep(2)
                    try:
                        entry_price = tsl.get_executed_price(orderid=entry_orderid)
                        if entry_price and isinstance(entry_price, (int, float)):
                            print(f"✅ Entry Price: ₹{entry_price}")
                            break
                        else:
                            print(f"⚠️ Waiting for execution... (Attempt {attempt+1}/5)")
                    except Exception as e:
                        print(f"⚠️ Price fetch error: {e}")
                
                if not entry_price or not isinstance(entry_price, (int, float)):
                    print(f"❌ Could not get entry price after retries!")
                    continue
                
                orderbook[trading_symbol]['entry_price'] = entry_price
                orderbook[trading_symbol]['tg'] = round(orderbook[trading_symbol]['entry_price']*1.002, 1)
                orderbook[trading_symbol]['sl'] = round(orderbook[trading_symbol]['entry_price']*0.998, 1)
                
                print(f"📊 TG: ₹{orderbook[trading_symbol]['tg']}, SL: ₹{orderbook[trading_symbol]['sl']}")
                
                # PLACE STOP LOSS ORDER WITH RETRY
                sl_orderid = None
                sl_price = int(orderbook[trading_symbol]['sl'])  # Convert to integer for Dhan MCX
                print(f"📤 Placing SL order at ₹{sl_price}...")
                
                for sl_attempt in range(3):
                    try:
                        sl_orderid = tsl.order_placement(
                            tradingsymbol=trading_symbol,
                            exchange='MCX',
                            quantity=1,
                            price=0,
                            trigger_price=sl_price,
                            order_type='STOPMARKET',
                            transaction_type='SELL',
                            trade_type='MIS'
                        )
                        
                        if sl_orderid and not isinstance(sl_orderid, dict):
                            print(f"✅ SL Order ID: {sl_orderid}")
                            break
                        else:
                            print(f"⚠️ SL order attempt {sl_attempt+1}/3 failed: {sl_orderid}")
                            if sl_attempt < 2:
                                time.sleep(2)
                    except Exception as e:
                        print(f"❌ SL order error (attempt {sl_attempt+1}/3): {e}")
                        traceback.print_exc()
                        if sl_attempt < 2:
                            time.sleep(2)
                
                if not sl_orderid or isinstance(sl_orderid, dict):
                    print(f"⚠️ SL order placement failed after 3 attempts, continuing without SL...")
                    sl_orderid = None
                else:
                    print(f"✅ SL Order ID: {sl_orderid}")
                
                orderbook[trading_symbol]['sl_orderid'] = sl_orderid
                orderbook[trading_symbol]['traded'] = "yes"
                
                message = "\n".join(f"'{key}': {repr(value)}" for key, value in orderbook[trading_symbol].items())
                print(f"✅ Order placed!")
                tsl.send_telegram_alert(message=f"✅ ENTRY - {trading_symbol}\n(Signal from {scan_name})\n\n{message}", receiver_chat_id=receiver_chat_id, bot_token=bot_token)
                tsl.send_telegram_alert(message=f"✅ ENTRY - {trading_symbol}\n(Signal from {scan_name})\n\n{message}", receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)
            except Exception as e:
                print(f"❌ Order failed: {e}")
                traceback.print_exc()
        
        if orderbook[trading_symbol]['traded'] == "yes":
            try:
                # SAFE LTP ACCESS
                if trading_symbol not in all_ltp:
                    print(f"⚠️ Cannot get {trading_symbol} LTP")
                    time.sleep(10)
                    continue
                
                ltp = all_ltp[trading_symbol]
                if not isinstance(ltp, (int, float)):
                    print(f"⚠️ Invalid LTP: {ltp}")
                    time.sleep(10)
                    continue
                
                sl_hit = tsl.get_order_status(orderid=orderbook[trading_symbol]['sl_orderid']) == "TRADED"
                tg_hit = ltp > orderbook[trading_symbol]['tg']
                max_holding_time_exceeded = datetime.datetime.now() > orderbook[trading_symbol]['max_holding_time']
                current_pnl = round((ltp - orderbook[trading_symbol]['entry_price'])*orderbook[trading_symbol]['qty'], 1)
                
                print(f"   📍 Entry={orderbook[trading_symbol]['entry_price']}, LTP={ltp}, PNL=₹{current_pnl}")
                
                if sl_hit:
                    orderbook[trading_symbol]['exit_time'] = str(current_time.time())[:8]
                    orderbook[trading_symbol]['exit_price'] = tsl.get_executed_price(orderid=orderbook[trading_symbol]['sl_orderid'])
                    orderbook[trading_symbol]['pnl'] = round((orderbook[trading_symbol]['exit_price'] - orderbook[trading_symbol]['entry_price']), 1)
                    orderbook[trading_symbol]['remark'] = "SL_hit"
                    print(f"🛑 SL Hit! PNL: ₹{orderbook[trading_symbol]['pnl']}")
                    tsl.send_telegram_alert(message=f"🛑 SL HIT - {trading_symbol}", receiver_chat_id=receiver_chat_id, bot_token=bot_token)
                    tsl.send_telegram_alert(message=f"🛑 SL HIT - {trading_symbol}", receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)
                    if reentry == "yes":
                        completed_orders.append(orderbook[trading_symbol])
                        orderbook[trading_symbol] = single_order.copy()
                
                if tg_hit:
                    print(f"📤 Target hit! Cancelling SL and placing SELL order...")
                    tsl.cancel_order(OrderID=orderbook[trading_symbol]['sl_orderid'])
                    time.sleep(2)
                    
                    square_off_order = None
                    for exit_attempt in range(3):
                        try:
                            square_off_order = tsl.order_placement(
                                tradingsymbol=trading_symbol, 
                                exchange='MCX', 
                                quantity=1, 
                                price=0, 
                                trigger_price=0, 
                                order_type='MARKET', 
                                transaction_type='SELL', 
                                trade_type='MIS'
                            )
                            if square_off_order and not isinstance(square_off_order, dict):
                                break
                            else:
                                print(f"⚠️ Exit attempt {exit_attempt+1}/3 failed")
                                time.sleep(2)
                        except Exception as e:
                            print(f"❌ Exit order error: {e}")
                            time.sleep(2)
                    
                    if square_off_order and not isinstance(square_off_order, dict):
                        orderbook[trading_symbol]['exit_time'] = str(current_time.time())[:8]
                        orderbook[trading_symbol]['exit_price'] = tsl.get_executed_price(orderid=square_off_order)
                        orderbook[trading_symbol]['pnl'] = orderbook[trading_symbol]['exit_price'] - orderbook[trading_symbol]['entry_price']
                        orderbook[trading_symbol]['remark'] = "TG_hit"
                        print(f"🎯 Target! PNL: ₹{orderbook[trading_symbol]['pnl']}")
                        tsl.send_telegram_alert(message=f"🎯 TARGET - {trading_symbol}", receiver_chat_id=receiver_chat_id, bot_token=bot_token)
                        tsl.send_telegram_alert(message=f"🎯 TARGET - {trading_symbol}", receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)
                        if reentry == "yes":
                            completed_orders.append(orderbook[trading_symbol])
                            orderbook[trading_symbol] = single_order.copy()
                    else:
                        print(f"❌ Could not place exit order at target")
                
                if max_holding_time_exceeded and (current_pnl < 0):
                    print(f"📤 Time exceeded with negative PNL. Cancelling SL and placing SELL order...")
                    tsl.cancel_order(OrderID=orderbook[trading_symbol]['sl_orderid'])
                    time.sleep(2)
                    
                    square_off_order = None
                    for exit_attempt in range(3):
                        try:
                            square_off_order = tsl.order_placement(
                                tradingsymbol=trading_symbol, 
                                exchange='MCX', 
                                quantity=1, 
                                price=0, 
                                trigger_price=0, 
                                order_type='MARKET', 
                                transaction_type='SELL', 
                                trade_type='MIS'
                            )
                            if square_off_order and not isinstance(square_off_order, dict):
                                break
                            else:
                                print(f"⚠️ Exit attempt {exit_attempt+1}/3 failed")
                                time.sleep(2)
                        except Exception as e:
                            print(f"❌ Exit order error: {e}")
                            time.sleep(2)
                    
                    if square_off_order and not isinstance(square_off_order, dict):
                        orderbook[trading_symbol]['exit_time'] = str(current_time.time())[:8]
                        orderbook[trading_symbol]['exit_price'] = tsl.get_executed_price(orderid=square_off_order)
                        orderbook[trading_symbol]['pnl'] = orderbook[trading_symbol]['exit_price'] - orderbook[trading_symbol]['entry_price']
                        orderbook[trading_symbol]['remark'] = "Time_exit"
                        print(f"⏱️ Time exit. PNL: ₹{orderbook[trading_symbol]['pnl']}")
                        tsl.send_telegram_alert(message=f"⏱️ TIME EXIT - {trading_symbol}", receiver_chat_id=receiver_chat_id, bot_token=bot_token)
                        tsl.send_telegram_alert(message=f"⏱️ TIME EXIT - {trading_symbol}", receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)
                        if reentry == "yes":
                            completed_orders.append(orderbook[trading_symbol])
                            orderbook[trading_symbol] = single_order.copy()
                    else:
                        print(f"❌ Could not place time exit order")
            except Exception as e:
                print(f"❌ Exit error: {e}")
                pass
    
    print("\n⏸️ Waiting 10 seconds...\n")
    time.sleep(10)

print("\n🛑 BOT STOPPED\n")
