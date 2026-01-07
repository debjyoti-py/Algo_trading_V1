import pdb
import time
import datetime
import traceback
import json
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
print("🚀 DHAN MCX TRADING BOT - GOLD Scanner + GOLDPETAL Trading (BUY/SELL)")
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

SCAN_SYMBOL_OPTIONS = ['GOLD FEB FUT', 'GOLD05FEB26FUT', 'GOLDFEB26FUT', 
                        'GOLD APR FUT', 'GOLD05APR26FUT', 'GOLDAPR26FUT', 'GOLD']

TRADING_SYMBOL_OPTIONS = ['GOLDPETAL JAN FUT', 'GOLDPETAL31JAN26FUT', 
                          'GOLDPETAL APR FUT', 'GOLDPETAL30APR26FUT', 
                          'GOLDPETALAPR26FUT', 'GOLDPETAL']

# Test GOLD scan symbol
print("🔍 Testing GOLD scan symbol formats...")
scan_symbol = None
for symbol in SCAN_SYMBOL_OPTIONS:
    try:
        test_gold = tsl.get_ltp_data(names=[symbol])
        if not isinstance(test_gold, dict) or test_gold.get('status') != 'failure':
            if symbol in test_gold and isinstance(test_gold[symbol], (int, float)) and test_gold[symbol] > 0:
                scan_symbol = symbol
                print(f"✅ Valid GOLD scan symbol found: {symbol}")
                break
    except:
        continue

if not scan_symbol:
    print(f"❌ GOLD scan symbol not found!")
    exit()

# Test GOLDPETAL trading symbol
print("\n🔍 Testing GOLDPETAL trading symbol formats...")
trading_symbol = None
for symbol in TRADING_SYMBOL_OPTIONS:
    try:
        test_data = tsl.get_ltp_data(names=[symbol])
        if not isinstance(test_data, dict) or test_data.get('status') != 'failure':
            if symbol in test_data and isinstance(test_data[symbol], (int, float)) and test_data[symbol] > 0:
                trading_symbol = symbol
                print(f"✅ Valid trading symbol found: {symbol}")
                break
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

def save_order_to_json(order_data, filename='order_history.json'):
    """Save order data to JSON file"""
    try:
        try:
            with open(filename, 'r') as f:
                orders = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            orders = []
        orders.append(order_data)
        with open(filename, 'w') as f:
            json.dump(orders, f, indent=2)
        print(f"📝 Order saved to {filename}")
    except Exception as e:
        print(f"❌ Error saving to JSON: {e}")

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
                tsl.send_telegram_alert(message=f"🔴 BOT STOPPED - Token Expired\nAPI: {api_name}", 
                                       receiver_chat_id=receiver_chat_id, bot_token=bot_token)
                tsl.send_telegram_alert(message=f"🔴 BOT STOPPED - Token Expired\nAPI: {api_name}", 
                                       receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)
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

        if isinstance(all_ltp, dict) and all_ltp.get('status') == 'failure':
            print(f"⚠️ LTP API failed: {all_ltp}")
            time.sleep(10)
            continue

        missing_symbols = []
        if scan_symbol not in all_ltp:
            missing_symbols.append(scan_symbol)
        if trading_symbol not in all_ltp:
            missing_symbols.append(trading_symbol)

        if missing_symbols:
            print(f"⚠️ LTP unavailable for: {', '.join(missing_symbols)}")
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
            chart['MACD'], chart['MACD_Signal'], chart['MACD_Hist'] = talib.MACD(chart['close'], 
                                                                                    fastperiod=12, 
                                                                                    slowperiod=26, 
                                                                                    signalperiod=9)

            # Calculate OBV and its SMA
            chart['obv'] = talib.OBV(chart['close'], chart['volume'])
            chart['obv_sma'] = talib.SMA(chart['obv'], timeperiod=50)

            # BUY signal - OBV bullish conditions
            chart['obv_above_sma'] = chart['obv'] > chart['obv_sma']
            chart['obv_crossover'] = (chart['obv'] > chart['obv_sma']) & (chart['obv'].shift(1) <= chart['obv_sma'].shift(1))
            chart['buy_signal'] = chart['obv_above_sma'] | chart['obv_crossover']

            # SELL signal - OBV bearish conditions
            chart['obv_below_sma'] = chart['obv'] < chart['obv_sma']
            chart['obv_crossunder'] = (chart['obv'] < chart['obv_sma']) & (chart['obv'].shift(1) >= chart['obv_sma'].shift(1))
            chart['sell_signal'] = chart['obv_below_sma'] | chart['obv_crossunder']

            sqn_lib.sqn(df=chart, period=21)
            chart['market_type'] = chart['sqn'].apply(sqn_lib.market_type)

            cc = chart.iloc[-1]
            prev_cc = chart.iloc[-3]

            # BUY signal conditions
            buy_c1 = cc['rsi'] > 65
            buy_c2 = orderbook[trading_symbol]['traded'] is None
            buy_c3 = cc['buy_signal']  # OBV bullish condition
            buy_c4 = cc['MACD'] > cc['MACD_Signal']

            # SELL signal conditions
            sell_c1 = cc['rsi'] < 40
            sell_c2 = orderbook[trading_symbol]['traded'] is None
            sell_c3 = cc['sell_signal']  # OBV bearish condition
            sell_c4 = cc['MACD'] < cc['MACD_Signal']

            # MACD Zone Detection
            macd_value = cc['MACD']
            macd_signal = cc['MACD_Signal']
            macd_diff = macd_value - macd_signal
            prev_macd_diff = prev_cc['MACD'] - prev_cc['MACD_Signal']

            if macd_value > 0:
                macd_zone = "Positive"
            else:
                macd_zone = "Negative"

            if macd_diff > 0 and prev_macd_diff <= 0:
                macd_status = "Bullish Crossover"
            elif macd_diff < 0 and prev_macd_diff >= 0:
                macd_status = "Bearish Crossover"
            elif macd_diff > 0:
                macd_status = "Above Signal"
            else:
                macd_status = "Below Signal"

            # OBV status
            obv_value = cc['obv']
            obv_sma_value = cc['obv_sma']
            obv_above = cc['obv_above_sma']
            obv_below = cc['obv_below_sma']
            obv_cross_up = cc['obv_crossover']
            obv_cross_down = cc['obv_crossunder']

            if obv_cross_up:
                obv_status = "Bullish Crossover ✅"
            elif obv_cross_down:
                obv_status = "Bearish Crossunder ❌"
            elif obv_above:
                obv_status = "Above SMA (Bullish)"
            elif obv_below:
                obv_status = "Below SMA (Bearish)"
            else:
                obv_status = "At SMA"

            print(f"   📊 RSI={cc['rsi']:.2f} | Market={cc['market_type']}")
            print(f"   📈 MACD={macd_value:.2f} | Signal={macd_signal:.2f} | Zone={macd_zone} | Status={macd_status}")
            print(f"   📊 OBV={obv_value:,.0f} | SMA={obv_sma_value:,.0f} | Status={obv_status}")
            print(f"   🎯 BUY: RSI>65={buy_c1} | OBV+={buy_c3} | MACD+={buy_c4}")
            print(f"   🎯 SELL: RSI<40={sell_c1} | OBV-={sell_c3} | MACD-={sell_c4}")

        except Exception as e:
            print(f"❌ Error: {e}")
            traceback.print_exc()
            continue

        # === BUY ENTRY LOGIC ===
        if buy_c1 and buy_c2 and buy_c3 and buy_c4:
            print(f"\n🎯 BUY SIGNAL FROM {scan_name}!")
            print(f"📈 Placing BUY order in {trading_symbol}...")

            try:
                if trading_symbol not in all_ltp:
                    print(f"❌ Cannot get {trading_symbol} price")
                    continue

                trading_ltp = all_ltp[trading_symbol]
                if not isinstance(trading_ltp, (int, float)):
                    print(f"❌ Invalid price: {trading_ltp}")
                    continue

                margin_available = tsl.get_balance()
                margin_required = trading_ltp/8

                if margin_available < margin_required:
                    shortfall = margin_required - margin_available
                    print(f"⚠️ LOW MARGIN! Need ₹{shortfall:,.2f} more")
                    continue

                orderbook[trading_symbol]['name'] = trading_symbol
                orderbook[trading_symbol]['date'] = str(current_time.date())
                orderbook[trading_symbol]['entry_time'] = str(current_time.time())[:8]
                orderbook[trading_symbol]['buy_sell'] = "BUY"
                orderbook[trading_symbol]['qty'] = 4

                entry_orderid = None
                for attempt in range(3):
                    try:
                        print(f"📤 Placing BUY order (Attempt {attempt+1}/3)...")
                        entry_orderid = tsl.order_placement(
                            tradingsymbol=trading_symbol,
                            exchange='MCX',
                            quantity=4,
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
                orderbook[trading_symbol]['tg'] = round(entry_price*1.002, 1)
                orderbook[trading_symbol]['sl'] = round(entry_price*0.998, 1)
                print(f"📊 TG: ₹{orderbook[trading_symbol]['tg']}, SL: ₹{orderbook[trading_symbol]['sl']}")

                # Place Stop Loss
                sl_orderid = None
                sl_price = int(orderbook[trading_symbol]['sl'])
                print(f"📤 Placing SL order at ₹{sl_price}...")

                for sl_attempt in range(3):
                    try:
                        sl_orderid = tsl.order_placement(
                            tradingsymbol=trading_symbol,
                            exchange='MCX',
                            quantity=4,
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
                        if sl_attempt < 2:
                            time.sleep(2)

                if not sl_orderid or isinstance(sl_orderid, dict):
                    print(f"⚠️ SL order placement failed, continuing without SL...")
                    sl_orderid = None
                else:
                    print(f"✅ SL Order ID: {sl_orderid}")

                orderbook[trading_symbol]['sl_orderid'] = sl_orderid
                orderbook[trading_symbol]['traded'] = "yes"

                order_entry = {
                    'entry_order_id': entry_orderid,
                    'entry_price': entry_price,
                    'date': str(current_time.date()),
                    'entry_time': str(current_time.time())[:8],
                    'sl_order_id': sl_orderid,
                    'target_price': orderbook[trading_symbol]['tg'],
                    'stop_loss_price': orderbook[trading_symbol]['sl'],
                    'symbol': trading_symbol,
                    'scan_symbol': scan_name,
                    'quantity': 4,
                    'direction': 'BUY'
                }
                save_order_to_json(order_entry)

                message = "\n".join(f"'{key}': {repr(value)}" for key, value in orderbook[trading_symbol].items())
                print(f"✅ BUY Order placed!")

                tsl.send_telegram_alert(message=f"✅ BUY ENTRY - {trading_symbol}\n(Signal from {scan_name})\n\n{message}", 
                                       receiver_chat_id=receiver_chat_id, bot_token=bot_token)
                tsl.send_telegram_alert(message=f"✅ BUY ENTRY - {trading_symbol}\n(Signal from {scan_name})\n\n{message}", 
                                       receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)

            except Exception as e:
                print(f"❌ BUY Order failed: {e}")
                traceback.print_exc()

        # === SELL ENTRY LOGIC ===
        elif sell_c1 and sell_c2 and sell_c3 and sell_c4:
            print(f"\n🎯 SELL SIGNAL FROM {scan_name}!")
            print(f"📉 Placing SELL order in {trading_symbol}...")

            try:
                if trading_symbol not in all_ltp:
                    print(f"❌ Cannot get {trading_symbol} price")
                    continue

                trading_ltp = all_ltp[trading_symbol]
                if not isinstance(trading_ltp, (int, float)):
                    print(f"❌ Invalid price: {trading_ltp}")
                    continue

                margin_available = tsl.get_balance()
                print(f"   💰 Available Balance: ₹{margin_available:,.2f}")

                orderbook[trading_symbol]['name'] = trading_symbol
                orderbook[trading_symbol]['date'] = str(current_time.date())
                orderbook[trading_symbol]['entry_time'] = str(current_time.time())[:8]
                orderbook[trading_symbol]['buy_sell'] = "SELL"
                orderbook[trading_symbol]['qty'] = 4

                print(f"\n   🔵 Placing SELL order...")

                try:
                    entry_orderid = tsl.order_placement(
                        tradingsymbol=trading_symbol,
                        exchange='MCX',
                        quantity=4,
                        price=0,
                        trigger_price=0,
                        order_type='MARKET',
                        transaction_type='SELL',
                        trade_type='MIS'
                    )
                    print(f"\n   📤 Order Response: {entry_orderid}")
                except Exception as order_error:
                    print(f"\n   ❌ Order Exception: {order_error}")
                    traceback.print_exc()
                    continue

                if entry_orderid is None:
                    print(f"\n   ❌ Order placement returned None")
                    continue

                if isinstance(entry_orderid, dict):
                    if entry_orderid.get('status') == 'failure':
                        error_details = entry_orderid.get('remarks', {})
                        print(f"\n   ❌ Order Rejected")
                        print(f"   Code: {error_details.get('error_code', 'N/A')}")
                        print(f"   Message: {error_details.get('error_message', 'N/A')}")
                        continue

                if isinstance(entry_orderid, (int, float)):
                    entry_orderid = str(int(entry_orderid))

                if not isinstance(entry_orderid, str) or entry_orderid == '':
                    print(f"\n   ❌ Invalid Order ID: {entry_orderid}")
                    continue

                print(f"\n   ✅ Order ID: {entry_orderid}")
                print(f"   ⏳ Waiting 5 seconds for execution...")
                time.sleep(5)

                try:
                    order_status = tsl.get_order_status(orderid=entry_orderid)
                    print(f"   📊 Order Status: {order_status}")
                    if order_status not in ["TRADED", "TRANSIT"]:
                        print(f"\n   ⚠️ Order not executed: {order_status}")
                        continue
                except Exception as status_error:
                    print(f"   ⚠️ Status check error: {status_error}")

                try:
                    entry_price = tsl.get_executed_price(orderid=entry_orderid)
                    if entry_price is None or entry_price == 0:
                        print(f"   ⏳ Retrying price fetch...")
                        time.sleep(3)
                        entry_price = tsl.get_executed_price(orderid=entry_orderid)

                    if entry_price is None or entry_price == 0:
                        print(f"\n   ❌ Cannot get executed price")
                        continue

                    orderbook[trading_symbol]['entry_orderid'] = entry_orderid
                    orderbook[trading_symbol]['entry_price'] = entry_price
                    orderbook[trading_symbol]['tg'] = round(entry_price * 0.998, 1)
                    orderbook[trading_symbol]['sl'] = round(entry_price * 1.002, 1)

                    print(f"\n   ✅ Entry Executed Successfully!")
                    print(f"   Entry Price: ₹{entry_price:,.2f}")
                    print(f"   Target: ₹{orderbook[trading_symbol]['tg']:,.2f} (-0.2%)")
                    print(f"   Stop Loss: ₹{orderbook[trading_symbol]['sl']:,.2f} (+0.2%)")

                except Exception as price_error:
                    print(f"\n   ❌ Price error: {price_error}")
                    traceback.print_exc()
                    continue

                # Place SL
                try:
                    print(f"\n   🛡️ Placing Stop Loss...")
                    sl_orderid = tsl.order_placement(
                        tradingsymbol=trading_symbol,
                        exchange='MCX',
                        quantity=4,
                        price=0,
                        trigger_price=orderbook[trading_symbol]['sl'],
                        order_type='STOPMARKET',
                        transaction_type='BUY',
                        trade_type='MIS'
                    )

                    if sl_orderid and sl_orderid != '':
                        orderbook[trading_symbol]['sl_orderid'] = sl_orderid
                        print(f"   ✅ SL Order ID: {sl_orderid}")
                    else:
                        print(f"   ⚠️ SL placement failed!")
                        orderbook[trading_symbol]['sl_orderid'] = None
                except Exception as sl_error:
                    print(f"   ⚠️ SL error: {sl_error}")
                    orderbook[trading_symbol]['sl_orderid'] = None

                orderbook[trading_symbol]['traded'] = "yes"

                order_entry = {
                    'entry_order_id': entry_orderid,
                    'entry_price': entry_price,
                    'date': str(current_time.date()),
                    'entry_time': orderbook[trading_symbol]['entry_time'],
                    'sl_order_id': orderbook[trading_symbol]['sl_orderid'],
                    'target_price': orderbook[trading_symbol]['tg'],
                    'stop_loss_price': orderbook[trading_symbol]['sl'],
                    'symbol': trading_symbol,
                    'scan_symbol': scan_name,
                    'quantity': 4,
                    'direction': 'SELL'
                }
                save_order_to_json(order_entry)

                message = "\n".join(f"{key}: {value}" for key, value in orderbook[trading_symbol].items())
                print(f"\n   ✅ SELL TRADE COMPLETE!")

                try:
                    tsl.send_telegram_alert(
                        message=f"✅ SELL ENTRY - {trading_symbol}\nSignal: {scan_name}\n\n{message}",
                        receiver_chat_id=receiver_chat_id,
                        bot_token=bot_token
                    )
                    tsl.send_telegram_alert(
                        message=f"✅ SELL ENTRY - {trading_symbol}\nSignal: {scan_name}\n\n{message}",
                        receiver_chat_id=receiver_chat_id_2,
                        bot_token=bot_token
                    )
                except:
                    pass

            except Exception as e:
                print(f"\n   ❌ Critical error: {e}")
                traceback.print_exc()
                continue

        # === EXIT LOGIC (Works for both BUY and SELL) ===
        if orderbook[trading_symbol]['traded'] == "yes":
            try:
                if trading_symbol not in all_ltp:
                    print(f"⚠️ Cannot get {trading_symbol} LTP")
                    time.sleep(10)
                    continue

                ltp = all_ltp[trading_symbol]
                if not isinstance(ltp, (int, float)):
                    print(f"⚠️ Invalid LTP: {ltp}")
                    time.sleep(10)
                    continue

                sl_hit = False
                if orderbook[trading_symbol]['sl_orderid'] is not None:
                    try:
                        sl_hit = tsl.get_order_status(orderid=orderbook[trading_symbol]['sl_orderid']) == "TRADED"
                    except:
                        pass

                direction = orderbook[trading_symbol]['buy_sell']

                if direction == "BUY":
                    tg_hit = ltp > orderbook[trading_symbol]['tg']
                    current_pnl = round((ltp - orderbook[trading_symbol]['entry_price'])*orderbook[trading_symbol]['qty'], 1)
                else:  # SELL
                    tg_hit = ltp <= orderbook[trading_symbol]['tg']
                    current_pnl = round((orderbook[trading_symbol]['entry_price'] - ltp)*orderbook[trading_symbol]['qty'], 1)

                print(f"   📍 Entry={orderbook[trading_symbol]['entry_price']}, LTP={ltp}, PNL=₹{current_pnl}")

                if sl_hit:
                    orderbook[trading_symbol]['exit_time'] = str(current_time.time())[:8]
                    orderbook[trading_symbol]['exit_price'] = tsl.get_executed_price(orderid=orderbook[trading_symbol]['sl_orderid'])

                    if direction == "BUY":
                        orderbook[trading_symbol]['pnl'] = round((orderbook[trading_symbol]['exit_price'] - orderbook[trading_symbol]['entry_price']), 1)
                    else:  # SELL
                        orderbook[trading_symbol]['pnl'] = round((orderbook[trading_symbol]['entry_price'] - orderbook[trading_symbol]['exit_price']), 1)

                    orderbook[trading_symbol]['remark'] = "SL_hit"
                    print(f"🛑 SL Hit! PNL: ₹{orderbook[trading_symbol]['pnl']}")

                    order_exit = {
                        'entry_order_id': orderbook[trading_symbol]['entry_orderid'],
                        'entry_price': orderbook[trading_symbol]['entry_price'],
                        'date': str(current_time.date()),
                        'entry_time': orderbook[trading_symbol]['entry_time'],
                        'sl_order_id': orderbook[trading_symbol]['sl_orderid'],
                        'exit_price': orderbook[trading_symbol]['exit_price'],
                        'exit_time': orderbook[trading_symbol]['exit_time'],
                        'pnl': orderbook[trading_symbol]['pnl'],
                        'remark': 'SL_hit',
                        'symbol': trading_symbol
                    }
                    save_order_to_json(order_exit)

                    tsl.send_telegram_alert(message=f"🛑 SL HIT - {trading_symbol}", 
                                           receiver_chat_id=receiver_chat_id, bot_token=bot_token)
                    tsl.send_telegram_alert(message=f"🛑 SL HIT - {trading_symbol}", 
                                           receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)

                    if reentry == "yes":
                        completed_orders.append(orderbook[trading_symbol])
                        orderbook[trading_symbol] = single_order.copy()

                if tg_hit:
                    print(f"📤 Target hit! Cancelling SL and placing square-off order...")
                    if orderbook[trading_symbol]['sl_orderid'] is not None:
                        try:
                            tsl.cancel_order(OrderID=orderbook[trading_symbol]['sl_orderid'])
                        except:
                            pass
                    time.sleep(2)

                    square_off_order = None
                    exit_transaction = 'SELL' if direction == 'BUY' else 'BUY'

                    for exit_attempt in range(3):
                        try:
                            square_off_order = tsl.order_placement(
                                tradingsymbol=trading_symbol,
                                exchange='MCX',
                                quantity=4,
                                price=0,
                                trigger_price=0,
                                order_type='MARKET',
                                transaction_type=exit_transaction,
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

                        if direction == "BUY":
                            orderbook[trading_symbol]['pnl'] = orderbook[trading_symbol]['exit_price'] - orderbook[trading_symbol]['entry_price']
                        else:  # SELL
                            orderbook[trading_symbol]['pnl'] = orderbook[trading_symbol]['entry_price'] - orderbook[trading_symbol]['exit_price']

                        orderbook[trading_symbol]['remark'] = "TG_hit"
                        print(f"🎯 Target! PNL: ₹{orderbook[trading_symbol]['pnl']}")

                        order_exit = {
                            'entry_order_id': orderbook[trading_symbol]['entry_orderid'],
                            'entry_price': orderbook[trading_symbol]['entry_price'],
                            'date': str(current_time.date()),
                            'entry_time': orderbook[trading_symbol]['entry_time'],
                            'sl_order_id': orderbook[trading_symbol]['sl_orderid'],
                            'exit_price': orderbook[trading_symbol]['exit_price'],
                            'exit_time': orderbook[trading_symbol]['exit_time'],
                            'pnl': orderbook[trading_symbol]['pnl'],
                            'remark': 'TG_hit',
                            'symbol': trading_symbol
                        }
                        save_order_to_json(order_exit)

                        tsl.send_telegram_alert(message=f"🎯 TARGET - {trading_symbol}", 
                                               receiver_chat_id=receiver_chat_id, bot_token=bot_token)
                        tsl.send_telegram_alert(message=f"🎯 TARGET - {trading_symbol}", 
                                               receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)

                        if reentry == "yes":
                            completed_orders.append(orderbook[trading_symbol])
                            orderbook[trading_symbol] = single_order.copy()
                    else:
                        print(f"❌ Could not place exit order at target")

            except Exception as e:
                print(f"❌ Exit error: {e}")
                pass

    print("\n⏸️ Waiting 10 seconds...\n")
    time.sleep(10)

print("\n🛑 BOT STOPPED\n")
