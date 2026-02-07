"""
🚀 GOLDPETAL PAPER TRADING VERSION 🚀
=====================================
⚠️  NO REAL MONEY WILL BE USED ⚠️
- All orders are SIMULATED
- Telegram alerts: ENABLED ✅
- Google Sheets: ENABLED ✅
- JSON logging: paper_trade.json ✅
- Entry/Exit logic: UNCHANGED ✅
=====================================
"""

import pdb
import time
import datetime
import traceback
import json
import os
from Dhan_Tradehull import Tradehull
import pandas as pd
from pprint import pprint
import talib
import pandas_ta as ta
import gspread
from gspread_dataframe import set_with_dataframe
from credentials import client_code
from credentials import token_id
import winsound
import sqn_lib

client_code = client_code
token_id = token_id

print("="*70)
print("🚀 PAPER TRADING BOT - NO REAL MONEY (SIMULATION MODE)")
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

SCAN_SYMBOL_OPTIONS = ['GOLDPETAL FEB FUT', 'GOLDPETAL27FEB26FUT',
                       'GOLDPETAL MAY FUT', 'GOLDPETAL29MAY26FUT',
                       'GOLDPETALMAY26FUT', 'GOLDPETAL']

TRADING_SYMBOL_OPTIONS = ['GOLDPETAL FEB FUT', 'GOLDPETAL27FEB26FUT',
                          'GOLDPETAL MAY FUT', 'GOLDPETAL29MAY26FUT',
                          'GOLDPETALMAY26FUT', 'GOLDPETAL']

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
    print(f"❌ GOLDPETAL scan symbol not found!")
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

# === NEW STRUCTURE: Track positions by ORDER ID ===
orderbook = {}  # Key = entry_order_id, Value = position details
completed_orders = []
open_symbols = set()

# === GOOGLE SHEETS SETUP ===
print("\n📂 Connecting to Google Sheets...")
gc = None
sheet = None
live_trading_ws = None
completed_orders_ws = None

try:
    gc = gspread.service_account(filename='algo-484008-d5bac2a16537.json')
    sheet = gc.open('Gold_Trade_Data')
    live_trading_ws = sheet.worksheet('Live_Trading')
    completed_orders_ws = sheet.worksheet('completed_orders')
    live_trading_ws.clear()
    completed_orders_ws.clear()
    print("✅ Google Sheet connected successfully\n")
except FileNotFoundError:
    print("❌ Error: algo-484008-d5bac2a16537.json not found!")
    print("📝 Make sure your Google credentials JSON file is in the project folder")
    exit()
except gspread.exceptions.SpreadsheetNotFound:
    print("❌ Error: 'Gold_Trade_Data' Google Sheet not found!")
    print("📝 Steps to fix:")
    print("   1. Create a Google Sheet named 'Gold_Trade_Data'")
    print("   2. Create two worksheets: 'Live_Trading' and 'completed_orders'")
    print("   3. Share it with: algo-484008@appspot.gserviceaccount.com")
    exit()
except Exception as e:
    print(f"❌ Google Sheets error: {e}")
    print(f"\n📝 Troubleshooting:")
    print(f"   - Check if Google Sheet 'Gold_Trade_Data' exists")
    print(f"   - Verify worksheets 'Live_Trading' and 'completed_orders' exist")
    print(f"   - Check if sheet is shared with service account email")
    exit()

reentry = "yes"
bot_token = "8549724310:AAHOJhoxbl2NPzHblsi04cRVabjREadq-UU"
receiver_chat_id = "6193962152"
receiver_chat_id_2 = "1234522531"
consecutive_api_failures = 0
MAX_API_FAILURES = 3

print(f"💰 Max Loss: ₹{max_risk_for_today:,.2f}")
print(f"⏰ Started: {datetime.datetime.now()}\n")

def save_order_entry(order_data, filename='paper_trade.json'):
    """Save ENTRY order data to JSON file"""
    try:
        try:
            with open(filename, 'r') as f:
                orders = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            orders = []

        orders.append(order_data)

        with open(filename, 'w') as f:
            json.dump(orders, f, indent=2)

        print(f"📝 Entry saved to {filename}")
    except Exception as e:
        print(f"❌ Error saving entry to JSON: {e}")

def update_order_exit(entry_order_id, exit_data, filename='paper_trade.json'):
    """UPDATE existing entry record with exit information (NOT append)"""
    try:
        try:
            with open(filename, 'r') as f:
                orders = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            orders = []

        # Find the entry record and UPDATE it
        found = False
        for order in orders:
            if order.get('entry_order_id') == entry_order_id and 'exit_price' not in order:
                # Update THIS record with exit info
                order['exit_price'] = exit_data['exit_price']
                order['exit_time'] = exit_data['exit_time']
                order['pnl'] = exit_data['pnl']
                order['remark'] = exit_data['remark']
                found = True
                print(f"✅ Updated order {entry_order_id} with exit info")
                break

        if not found:
            print(f"⚠️ Warning: Entry record for {entry_order_id} not found, appending exit")
            exit_record = exit_data.copy()
            exit_record['entry_order_id'] = entry_order_id
            orders.append(exit_record)

        with open(filename, 'w') as f:
            json.dump(orders, f, indent=2)

        print(f"📝 Exit saved to {filename}")
    except Exception as e:
        print(f"❌ Error updating exit in JSON: {e}")
        traceback.print_exc()

def update_google_sheets():
    """Update Google Sheets with ALL live positions"""
    try:
        if orderbook:
            positions_list = []
            for order_id, position in orderbook.items():
                pos_dict = position.copy()
                pos_dict['order_id'] = order_id
                positions_list.append(pos_dict)

            orderbook_df = pd.DataFrame(positions_list)
            set_with_dataframe(live_trading_ws, orderbook_df, include_column_header=True)
            print(f"📊 Google Sheets updated - {len(orderbook)} open position(s)")
        else:
            live_trading_ws.clear()
            print(f"📊 Google Sheets cleared - No open positions")

        if completed_orders:
            completed_orders_df = pd.DataFrame(completed_orders)
            set_with_dataframe(completed_orders_ws, completed_orders_df, include_column_header=True)
    except Exception as e:
        print(f"⚠️ Google Sheets update error: {e}")

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

# === CHECK FOR EXISTING POSITIONS ===
print("\n" + "="*70)
print("🔍 CHECKING FOR EXISTING POSITIONS (ALL POSITIONS)...")
print("="*70)

try:
    with open('paper_trade.json', 'r') as f:
        orders = json.load(f)

    order_dict = {}
    for order in orders:
        entry_id = order.get('entry_order_id')
        if not entry_id:
            continue

        if entry_id not in order_dict:
            order_dict[entry_id] = order
        else:
            if 'exit_price' in order and 'exit_price' not in order_dict[entry_id]:
                order_dict[entry_id] = order

    json_open_positions = []
    for entry_id, order in order_dict.items():
        if 'exit_price' not in order:
            json_open_positions.append(order)

    if json_open_positions:
        print(f"\n⚠️ FOUND {len(json_open_positions)} OPEN POSITION(S) IN JSON FILE!")
        print("="*70)

        for idx, position in enumerate(json_open_positions, 1):
            entry_order_id = position.get('entry_order_id')
            symbol = position.get('symbol')

            orderbook[entry_order_id] = {
                'name': symbol,
                'date': position.get('date'),
                'entry_time': position.get('entry_time'),
                'entry_price': position.get('entry_price'),
                'buy_sell': position.get('direction'),
                'qty': position.get('quantity', 1),
                'sl': position.get('stop_loss_price'),
                'tg': position.get('target_price'),
                'entry_orderid': entry_order_id,
                'sl_orderid': position.get('sl_order_id'),
                'exit_time': None,
                'exit_price': None,
                'pnl': None,
                'remark': None,
                'traded': 'yes'
            }

            open_symbols.add(symbol)

            print(f"\nPosition #{idx}:")
            print(f"   📊 Symbol: {symbol}")
            print(f"   📈 Direction: {position.get('direction')}")
            print(f"   💰 Entry: ₹{position.get('entry_price'):,.2f}")
            print(f"   🎯 Target: ₹{position.get('target_price'):,.2f}")
            print(f"   🛡️ Stop Loss: ₹{position.get('stop_loss_price'):,.2f}")
            print(f"   ⏰ Entry Time: {position.get('entry_time')}")
            print(f"   🆔 Entry Order ID: {entry_order_id}")
            print(f"   🆔 SL Order ID: {position.get('sl_order_id')}")

        update_google_sheets()

        message = f"""⚠️ BOT RESTARTED - {len(json_open_positions)} OPEN POSITION(S) FOUND

Symbol: {trading_symbol}
Total Open Positions: {len(json_open_positions)}

"""
        for idx, pos in enumerate(json_open_positions, 1):
            message += f"""
Position #{idx}:
Entry: ₹{pos.get('entry_price'):,.2f} at {pos.get('entry_time')}
Direction: {pos.get('direction')}
SL: ₹{pos.get('stop_loss_price'):,.2f}
Target: ₹{pos.get('target_price'):,.2f}
Order ID: {pos.get('entry_order_id')}
"""

        message += "\n🚫 NEW ENTRIES BLOCKED FOR THIS SYMBOL"

        try:
            tsl.send_telegram_alert(message=message, receiver_chat_id=receiver_chat_id, bot_token=bot_token)
            tsl.send_telegram_alert(message=message, receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)
        except:
            pass
    else:
        print("✅ No open positions found in JSON")
except FileNotFoundError:
    print("✅ No order history file found")
except json.JSONDecodeError:
    print("⚠️ JSON file corrupted - starting fresh")
except Exception as e:
    print(f"⚠️ Error checking JSON: {e}")
    traceback.print_exc()

# Method 2: Check Dhan account
try:
    print("\n🔍 Checking Dhan account for open positions...")
    dhan_positions = tsl.get_positions()

    if isinstance(dhan_positions, list) and len(dhan_positions) > 0:
        dhan_open_count = 0
        for pos in dhan_positions:
            if isinstance(pos, dict):
                pos_symbol = pos.get('tradingSymbol', '')
                net_qty = pos.get('netQty', 0)

                if net_qty != 0 and trading_symbol in pos_symbol:
                    dhan_open_count += 1
                    open_symbols.add(trading_symbol)
                    print(f"\n⚠️ OPEN POSITION IN DHAN ACCOUNT:")
                    print(f"   📊 Symbol: {pos_symbol}")
                    print(f"   📦 Net Qty: {net_qty}")
                    print(f"   💰 Average Price: ₹{pos.get('avgPrice', 0):,.2f}")
                    print(f"   💵 Unrealized P&L: ₹{pos.get('unrealizedProfit', 0):,.2f}")

        if dhan_open_count > 0:
            print(f"\n✅ Total positions in Dhan: {dhan_open_count}")
        else:
            print("✅ No open positions in Dhan account")
    else:
        print("✅ No open positions in Dhan account")
except Exception as e:
    print(f"⚠️ Error checking Dhan positions: {e}")

if open_symbols:
    print("\n" + "="*70)
    print(f"🚫 {len(orderbook)} OPEN POSITION(S) DETECTED - NEW ENTRIES BLOCKED")
    print("="*70)
    print(f"Symbols with open positions: {', '.join(open_symbols)}")
    print("⏰ Bot will monitor all positions")
    print("⏰ New entries blocked until ALL positions close\n")
else:
    print("\n" + "="*70)
    print("✅ NO OPEN POSITIONS - BOT READY FOR NEW ENTRIES")
    print("="*70 + "\n")

# === MAIN TRADING LOOP ===
try:
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
            print(f"🛑 MAX LOSS HIT! PNL: ₹{live_pnl:,.2f}")
            pass

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
            update_google_sheets()

            print(f"\n🔍 Scanning {scan_name} at {current_time.strftime('%H:%M:%S')}")
            print(f"   📊 Open Positions: {len(orderbook)}")

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
                chart['obv'] = talib.OBV(chart['close'], chart['volume'])
                chart['obv_ema'] = talib.EMA(chart['obv'], timeperiod=50)
                chart['obv_above_ema'] = chart['obv'] > chart['obv_ema']
                chart['obv_crossover'] = (chart['obv'] > chart['obv_ema']) & (chart['obv'].shift(1) <= chart['obv_ema'].shift(1))
                chart['buy_signal'] = chart['obv_above_ema'] | chart['obv_crossover']
                chart['obv_below_ema'] = chart['obv'] < chart['obv_ema']
                chart['obv_crossunder'] = (chart['obv'] < chart['obv_ema']) & (chart['obv'].shift(1) >= chart['obv_ema'].shift(1))
                chart['sell_signal'] = chart['obv_below_ema'] | chart['obv_crossunder']

                sqn_lib.sqn(df=chart, period=21)
                chart['market_type'] = chart['sqn'].apply(sqn_lib.market_type)

                cc = chart.iloc[-1]
                prev_cc = chart.iloc[-3]

                # BUY signal conditions
                buy_c1 = cc['rsi'] > 65
                buy_c2 = trading_symbol not in open_symbols
                buy_c3 = cc['buy_signal']

                prev_macd_above = prev_cc['MACD'] > prev_cc['MACD_Signal']
                curr_macd_above = cc['MACD'] > cc['MACD_Signal']
                macd_crossover = (curr_macd_above) & (prev_cc['MACD'] <= prev_cc['MACD_Signal'])
                buy_c4 = (macd_crossover | curr_macd_above)

                # SELL signal conditions
                sell_c1 = cc['rsi'] < 35
                sell_c2 = trading_symbol not in open_symbols
                sell_c3 = cc['sell_signal']
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
                obv_ema_value = cc['obv_ema']
                obv_above = cc['obv_above_ema']
                obv_below = cc['obv_below_ema']
                obv_cross_up = cc['obv_crossover']
                obv_cross_down = cc['obv_crossunder']

                if obv_cross_up:
                    obv_status = "Bullish Crossover ✅"
                elif obv_cross_down:
                    obv_status = "Bearish Crossunder ❌"
                elif obv_above:
                    obv_status = "Above EMA (Bullish)"
                elif obv_below:
                    obv_status = "Below EMA (Bearish)"
                else:
                    obv_status = "At EMA"

                print(f"   📊 RSI={cc['rsi']:.2f} | Market={cc['market_type']}")
                print(f"   📈 MACD={macd_value:.2f} | Signal={macd_signal:.2f} | Zone={macd_zone} | Status={macd_status}")
                print(f"   📊 OBV={obv_value:,.0f} | EMA={obv_ema_value:,.0f} | Status={obv_status}")
                print(f"   🎯 BUY: RSI>65={buy_c1} | No_Open_Pos={buy_c2} | OBV+={buy_c3} | MACD+={buy_c4}")
                print(f"   🎯 SELL: RSI<35={sell_c1} | No_Open_Pos={sell_c2} | OBV-={sell_c3} | MACD-={sell_c4}")

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

                    # PAPER TRADING - Simulated order
                    entry_orderid = f"PAPER_{current_time.strftime('%Y%m%d_%H%M%S')}_BUY"
                    print(f"📝 PAPER TRADE: Simulated BUY order")
                    print(f"✅ Entry Order ID: {entry_orderid}")

                    # PAPER TRADING - Use LTP as entry price
                    entry_price = trading_ltp
                    print(f"📝 PAPER TRADE: Entry price = LTP = ₹{entry_price}")
                    print(f"✅ Entry Price: ₹{entry_price}")

                    target_price = round(entry_price*1.02, 1)  # 2% target
                    stop_loss_price = round(entry_price*0.99, 1)  # 1% stop loss
                    print(f"📊 TG: ₹{target_price}, SL: ₹{stop_loss_price}")

                    # PAPER TRADING - Simulated SL order
                    sl_orderid = f"PAPER_SL_{current_time.strftime('%Y%m%d_%H%M%S')}_BUY"
                    print(f"📝 PAPER TRADE: Simulated SL order")
                    print(f"✅ SL Order ID: {sl_orderid}")

                    # Add to orderbook
                    orderbook[entry_orderid] = {
                        'name': trading_symbol,
                        'date': str(current_time.date()),
                        'entry_time': str(current_time.time())[:8],
                        'entry_price': entry_price,
                        'buy_sell': 'BUY',
                        'qty': 4,
                        'sl': stop_loss_price,
                        'tg': target_price,
                        'entry_orderid': entry_orderid,
                        'sl_orderid': sl_orderid,
                        'exit_time': None,
                        'exit_price': None,
                        'pnl': None,
                        'remark': None,
                        'traded': 'yes',
                        'entry_rsi': cc['rsi'],
                        'entry_macd': macd_value,
                        'entry_macd_signal': macd_signal,
                        'entry_macd_status': macd_status,
                        'entry_obv': obv_value,
                        'entry_obv_ema': obv_ema_value,
                        'entry_obv_status': obv_status
                    }

                    open_symbols.add(trading_symbol)

                    order_entry = {
                        'entry_order_id': entry_orderid,
                        'entry_price': entry_price,
                        'date': str(current_time.date()),
                        'entry_time': str(current_time.time())[:8],
                        'sl_order_id': sl_orderid,
                        'target_price': target_price,
                        'stop_loss_price': stop_loss_price,
                        'symbol': trading_symbol,
                        'scan_symbol': scan_name,
                        'quantity': 4,
                        'direction': 'BUY'
                    }

                    save_order_entry(order_entry)

                    message = f"""🟢 BUY ENTRY (PAPER TRADE)

📅 Date: {str(current_time.date())}
📊 Symbol: {trading_symbol}
📍 Direction: BUY
📦 Quantity: 4
💰 Entry Price: ₹{entry_price:,.2f}
🎯 Target: ₹{target_price:,.2f}
🛑 Stop Loss: ₹{stop_loss_price:,.2f}
⏰ Entry Time: {str(current_time.time())[:8]}
📋 Entry Order ID: {entry_orderid}
📋 SL Order ID: {sl_orderid}

📊 INDICATORS AT ENTRY:
📈 RSI: {cc['rsi']:.2f}
📉 MACD: {macd_value:.2f} | Signal: {macd_signal:.2f}
📊 MACD Status: {macd_status}
📦 OBV: {obv_value:,.0f} | EMA: {obv_ema_value:,.0f}
✅ OBV Status: {obv_status}

📊 Total Open Positions: {len(orderbook)}"""

                    try:
                        tsl.send_telegram_alert(message=message, receiver_chat_id=receiver_chat_id, bot_token=bot_token)
                        tsl.send_telegram_alert(message=message, receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)
                    except:
                        pass

                except Exception as e:
                    print(f"❌ BUY Order failed: {e}")
                    traceback.print_exc()

            elif buy_c1 and buy_c3 and buy_c4 and not buy_c2:
                print(f"   🚫 BUY signal detected but {trading_symbol} has {len([p for p in orderbook.values() if p['name'] == trading_symbol])} OPEN POSITION(S) - Entry BLOCKED")

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
                    margin_required = trading_ltp/8

                    if margin_available < margin_required:
                        shortfall = margin_required - margin_available
                        print(f"⚠️ LOW MARGIN! Need ₹{shortfall:,.2f} more")
                        continue

                    # PAPER TRADING - Simulated order
                    entry_orderid = f"PAPER_{current_time.strftime('%Y%m%d_%H%M%S')}_SELL"
                    print(f"📝 PAPER TRADE: Simulated SELL order")
                    print(f"✅ Entry Order ID: {entry_orderid}")

                    # PAPER TRADING - Use LTP as entry price
                    entry_price = trading_ltp
                    print(f"📝 PAPER TRADE: Entry price = LTP = ₹{entry_price}")
                    print(f"✅ Entry Price: ₹{entry_price}")

                    target_price = round(entry_price*0.98, 1)  # 2% below entry
                    stop_loss_price = round(entry_price*1.01, 1)  # 1% above entry
                    print(f"📊 TG: ₹{target_price}, SL: ₹{stop_loss_price}")

                    # PAPER TRADING - Simulated SL order
                    sl_orderid = f"PAPER_SL_{current_time.strftime('%Y%m%d_%H%M%S')}_SELL"
                    print(f"📝 PAPER TRADE: Simulated SL order")
                    print(f"✅ SL Order ID: {sl_orderid}")

                    orderbook[entry_orderid] = {
                        'name': trading_symbol,
                        'date': str(current_time.date()),
                        'entry_time': str(current_time.time())[:8],
                        'entry_price': entry_price,
                        'buy_sell': 'SELL',
                        'qty': 4,
                        'sl': stop_loss_price,
                        'tg': target_price,
                        'entry_orderid': entry_orderid,
                        'sl_orderid': sl_orderid,
                        'exit_time': None,
                        'exit_price': None,
                        'pnl': None,
                        'remark': None,
                        'traded': 'yes',
                        'entry_rsi': cc['rsi'],
                        'entry_macd': macd_value,
                        'entry_macd_signal': macd_signal,
                        'entry_macd_status': macd_status,
                        'entry_obv': obv_value,
                        'entry_obv_ema': obv_ema_value,
                        'entry_obv_status': obv_status
                    }

                    open_symbols.add(trading_symbol)

                    order_entry = {
                        'entry_order_id': entry_orderid,
                        'entry_price': entry_price,
                        'date': str(current_time.date()),
                        'entry_time': str(current_time.time())[:8],
                        'sl_order_id': sl_orderid,
                        'target_price': target_price,
                        'stop_loss_price': stop_loss_price,
                        'symbol': trading_symbol,
                        'scan_symbol': scan_name,
                        'quantity': 4,
                        'direction': 'SELL'
                    }

                    save_order_entry(order_entry)

                    message = f"""🔴 SELL ENTRY (PAPER TRADE)

📅 Date: {str(current_time.date())}
📊 Symbol: {trading_symbol}
📍 Direction: SELL
📦 Quantity: 4
💰 Entry Price: ₹{entry_price:,.2f}
🎯 Target: ₹{target_price:,.2f}
🛑 Stop Loss: ₹{stop_loss_price:,.2f}
⏰ Entry Time: {str(current_time.time())[:8]}
📋 Entry Order ID: {entry_orderid}
📋 SL Order ID: {sl_orderid}

📊 INDICATORS AT ENTRY:
📈 RSI: {cc['rsi']:.2f}
📉 MACD: {macd_value:.2f} | Signal: {macd_signal:.2f}
📊 MACD Status: {macd_status}
📦 OBV: {obv_value:,.0f} | EMA: {obv_ema_value:,.0f}
✅ OBV Status: {obv_status}

📊 Total Open Positions: {len(orderbook)}"""

                    try:
                        tsl.send_telegram_alert(message=message, receiver_chat_id=receiver_chat_id, bot_token=bot_token)
                        tsl.send_telegram_alert(message=message, receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)
                    except:
                        pass

                except Exception as e:
                    print(f"❌ SELL Order failed: {e}")
                    traceback.print_exc()

            elif sell_c1 and sell_c3 and sell_c4 and not sell_c2:
                print(f"   🚫 SELL signal detected but {trading_symbol} has {len([p for p in orderbook.values() if p['name'] == trading_symbol])} OPEN POSITION(S) - Entry BLOCKED")

            # === EXIT LOGIC - Check ALL open positions ===
            if orderbook:
                positions_to_remove = []

                for order_id, position in orderbook.items():
                    try:
                        pos_symbol = position['name']

                        if pos_symbol not in all_ltp:
                            continue

                        ltp = all_ltp[pos_symbol]
                        if not isinstance(ltp, (int, float)):
                            continue

                        # PAPER TRADING - Simulated SL status check
                        sl_hit = False
                        # In paper trading, check if LTP hit SL
                        direction = position['buy_sell']
                        if direction == "BUY":
                            sl_hit = ltp <= position['sl']
                        else:
                            sl_hit = ltp >= position['sl']

                        if direction == "BUY":
                            tg_hit = ltp >= position['tg']
                            current_pnl = round((ltp - position['entry_price'])*position['qty'], 1)
                        else:
                            tg_hit = ltp <= position['tg']
                            current_pnl = round((position['entry_price'] - ltp)*position['qty'], 1)

                        print(f"   📍 Order {order_id[:10]}... | {direction} | Entry=₹{position['entry_price']} | LTP=₹{ltp} | PNL=₹{current_pnl}")

                        # === STOP LOSS HIT ===
                        if sl_hit:
                            # PAPER TRADING - Use SL price as exit price
                            exit_price = position['sl']
                            print(f"📝 PAPER TRADE: SL exit price = ₹{exit_price}")

                            if direction == "BUY":
                                pnl = round((exit_price - position['entry_price']) * position['qty'], 1)
                            else:
                                pnl = round((position['entry_price'] - exit_price) * position['qty'], 1)

                            print(f"\n🛑 SL HIT - Order {order_id}!")
                            print(f"   Direction: {direction}")
                            print(f"   Exit Price: ₹{exit_price:,.2f}")
                            print(f"   P&L: ₹{pnl:,.2f}")

                            # Calculate exit indicators
                            try:
                                exit_chart = tsl.get_historical_data(tradingsymbol=scan_name, exchange='MCX', timeframe="15")
                                if exit_chart is not None and len(exit_chart) >= 26:
                                    exit_chart['rsi'] = talib.RSI(exit_chart['close'], timeperiod=14)
                                    exit_chart['MACD'], exit_chart['MACD_Signal'], exit_chart['MACD_Hist'] = talib.MACD(
                                        exit_chart['close'], fastperiod=12, slowperiod=26, signalperiod=9
                                    )
                                    exit_chart['obv'] = talib.OBV(exit_chart['close'], exit_chart['volume'])
                                    exit_chart['obv_ema'] = talib.EMA(exit_chart['obv'], timeperiod=50)

                                    exit_cc = exit_chart.iloc[-1]
                                    exit_rsi = exit_cc['rsi']
                                    exit_macd = exit_cc['MACD']
                                    exit_macd_signal = exit_cc['MACD_Signal']
                                    exit_obv = exit_cc['obv']
                                    exit_obv_ema = exit_cc['obv_ema']

                                    if exit_macd > exit_macd_signal:
                                        exit_macd_status = "Above Signal (Bullish)"
                                    else:
                                        exit_macd_status = "Below Signal (Bearish)"

                                    if exit_obv > exit_obv_ema:
                                        exit_obv_status = "Above EMA (Bullish)"
                                    else:
                                        exit_obv_status = "Below EMA (Bearish)"
                                else:
                                    exit_rsi = "N/A"
                                    exit_macd = "N/A"
                                    exit_macd_signal = "N/A"
                                    exit_macd_status = "N/A"
                                    exit_obv = "N/A"
                                    exit_obv_ema = "N/A"
                                    exit_obv_status = "N/A"
                            except:
                                exit_rsi = "N/A"
                                exit_macd = "N/A"
                                exit_macd_signal = "N/A"
                                exit_macd_status = "N/A"
                                exit_obv = "N/A"
                                exit_obv_ema = "N/A"
                                exit_obv_status = "N/A"

                            exit_data = {
                                'exit_price': exit_price,
                                'exit_time': str(current_time.time())[:8],
                                'pnl': pnl,
                                'remark': 'SL_hit',
                                'symbol': pos_symbol
                            }

                            update_order_exit(order_id, exit_data)

                            sl_message = f"""🔴 STOP LOSS HIT (PAPER TRADE)

📅 Date: {position['date']}
📊 Symbol: {pos_symbol}
📍 Direction: {direction}
📦 Quantity: {position['qty']}
💰 Entry Price: ₹{position['entry_price']:,.2f}
💸 Exit Price: ₹{exit_price:,.2f}
🛑 Stop Loss: ₹{position['sl']:,.2f}
⏰ Entry Time: {position['entry_time']}
⏱️ Exit Time: {exit_data['exit_time']}
{'💔 Loss' if pnl < 0 else '💚 Profit'}: ₹{pnl:,.2f}
🔖 Remark: SL HIT

📊 INDICATORS AT ENTRY:
📈 RSI: {position.get('entry_rsi', 'N/A')}
📉 MACD: {position.get('entry_macd', 'N/A')} | Signal: {position.get('entry_macd_signal', 'N/A')}
📊 Status: {position.get('entry_macd_status', 'N/A')}
📦 OBV: {position.get('entry_obv', 'N/A')} | EMA: {position.get('entry_obv_ema', 'N/A')}
✅ Status: {position.get('entry_obv_status', 'N/A')}

📊 INDICATORS AT EXIT:
📈 RSI: {exit_rsi if exit_rsi == 'N/A' else f'{exit_rsi:.2f}'}
📉 MACD: {exit_macd if exit_macd == 'N/A' else f'{exit_macd:.2f}'} | Signal: {exit_macd_signal if exit_macd_signal == 'N/A' else f'{exit_macd_signal:.2f}'}
📊 Status: {exit_macd_status}
📦 OBV: {exit_obv if exit_obv == 'N/A' else f'{exit_obv:,.0f}'} | EMA: {exit_obv_ema if exit_obv_ema == 'N/A' else f'{exit_obv_ema:,.0f}'}
✅ Status: {exit_obv_status}

📋 Entry Order: {order_id}
📋 SL Order: {position['sl_orderid']}
📊 Remaining Positions: {len(orderbook) - 1}"""

                            try:
                                tsl.send_telegram_alert(message=sl_message, receiver_chat_id=receiver_chat_id, bot_token=bot_token)
                                tsl.send_telegram_alert(message=sl_message, receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)
                            except:
                                pass

                            position['exit_price'] = exit_price
                            position['exit_time'] = exit_data['exit_time']
                            position['pnl'] = pnl
                            position['remark'] = 'SL_hit'
                            completed_orders.append(position)
                            positions_to_remove.append(order_id)

                        # === TARGET HIT ===
                        elif tg_hit:
                            print(f"\n🎯 TARGET HIT - Order {order_id}!")
                            print(f"   📝 PAPER TRADE: Simulated square-off")

                            # PAPER TRADING - Use LTP as exit price
                            exit_price = ltp
                            print(f"📝 PAPER TRADE: Exit price = LTP = ₹{exit_price}")

                            # PAPER TRADING - Simulated square-off order
                            exit_transaction = 'SELL' if direction == 'BUY' else 'BUY'
                            square_off_order = f"PAPER_EXIT_{current_time.strftime('%Y%m%d_%H%M%S')}_{exit_transaction}"
                            print(f"📝 PAPER TRADE: Simulated exit order")
                            print(f"   ✅ Square-off Order ID: {square_off_order}")

                            if direction == "BUY":
                                pnl = round((exit_price - position['entry_price']) * position['qty'], 1)
                            else:
                                pnl = round((position['entry_price'] - exit_price) * position['qty'], 1)

                            print(f"   Direction: {direction}")
                            print(f"   Exit Price: ₹{exit_price:,.2f}")
                            print(f"   P&L: ₹{pnl:,.2f}")

                            # Calculate exit indicators
                            try:
                                exit_chart = tsl.get_historical_data(tradingsymbol=scan_name, exchange='MCX', timeframe="15")
                                if exit_chart is not None and len(exit_chart) >= 26:
                                    exit_chart['rsi'] = talib.RSI(exit_chart['close'], timeperiod=14)
                                    exit_chart['MACD'], exit_chart['MACD_Signal'], exit_chart['MACD_Hist'] = talib.MACD(
                                        exit_chart['close'], fastperiod=12, slowperiod=26, signalperiod=9
                                    )
                                    exit_chart['obv'] = talib.OBV(exit_chart['close'], exit_chart['volume'])
                                    exit_chart['obv_ema'] = talib.EMA(exit_chart['obv'], timeperiod=50)

                                    exit_cc = exit_chart.iloc[-1]
                                    exit_rsi = exit_cc['rsi']
                                    exit_macd = exit_cc['MACD']
                                    exit_macd_signal = exit_cc['MACD_Signal']
                                    exit_obv = exit_cc['obv']
                                    exit_obv_ema = exit_cc['obv_ema']

                                    if exit_macd > exit_macd_signal:
                                        exit_macd_status = "Above Signal (Bullish)"
                                    else:
                                        exit_macd_status = "Below Signal (Bearish)"

                                    if exit_obv > exit_obv_ema:
                                        exit_obv_status = "Above EMA (Bullish)"
                                    else:
                                        exit_obv_status = "Below EMA (Bearish)"
                                else:
                                    exit_rsi = "N/A"
                                    exit_macd = "N/A"
                                    exit_macd_signal = "N/A"
                                    exit_macd_status = "N/A"
                                    exit_obv = "N/A"
                                    exit_obv_ema = "N/A"
                                    exit_obv_status = "N/A"
                            except:
                                exit_rsi = "N/A"
                                exit_macd = "N/A"
                                exit_macd_signal = "N/A"
                                exit_macd_status = "N/A"
                                exit_obv = "N/A"
                                exit_obv_ema = "N/A"
                                exit_obv_status = "N/A"

                            exit_data = {
                                'exit_price': exit_price,
                                'exit_time': str(current_time.time())[:8],
                                'pnl': pnl,
                                'remark': 'TG_hit',
                                'symbol': pos_symbol
                            }

                            update_order_exit(order_id, exit_data)

                            tg_message = f"""🎯 TARGET HIT (PAPER TRADE)

📅 Date: {position['date']}
📊 Symbol: {pos_symbol}
📍 Direction: {direction}
📦 Quantity: {position['qty']}
💰 Entry Price: ₹{position['entry_price']:,.2f}
💸 Exit Price: ₹{exit_price:,.2f}
🎯 Target: ₹{position['tg']:,.2f}
⏰ Entry Time: {position['entry_time']}
⏱️ Exit Time: {exit_data['exit_time']}
{'💚 Profit' if pnl > 0 else '💔 Loss'}: ₹{pnl:,.2f}
🔖 Remark: TARGET HIT

📊 INDICATORS AT ENTRY:
📈 RSI: {position.get('entry_rsi', 'N/A')}
📉 MACD: {position.get('entry_macd', 'N/A')} | Signal: {position.get('entry_macd_signal', 'N/A')}
📊 Status: {position.get('entry_macd_status', 'N/A')}
📦 OBV: {position.get('entry_obv', 'N/A')} | EMA: {position.get('entry_obv_ema', 'N/A')}
✅ Status: {position.get('entry_obv_status', 'N/A')}

📊 INDICATORS AT EXIT:
📈 RSI: {exit_rsi if exit_rsi == 'N/A' else f'{exit_rsi:.2f}'}
📉 MACD: {exit_macd if exit_macd == 'N/A' else f'{exit_macd:.2f}'} | Signal: {exit_macd_signal if exit_macd_signal == 'N/A' else f'{exit_macd_signal:.2f}'}
📊 Status: {exit_macd_status}
📦 OBV: {exit_obv if exit_obv == 'N/A' else f'{exit_obv:,.0f}'} | EMA: {exit_obv_ema if exit_obv_ema == 'N/A' else f'{exit_obv_ema:,.0f}'}
✅ Status: {exit_obv_status}

📋 Entry Order: {order_id}
📋 Exit Order: {square_off_order}
📊 Remaining Positions: {len(orderbook) - 1}"""

                            try:
                                tsl.send_telegram_alert(message=tg_message, receiver_chat_id=receiver_chat_id, bot_token=bot_token)
                                tsl.send_telegram_alert(message=tg_message, receiver_chat_id=receiver_chat_id_2, bot_token=bot_token)
                            except:
                                pass

                            position['exit_price'] = exit_price
                            position['exit_time'] = exit_data['exit_time']
                            position['pnl'] = pnl
                            position['remark'] = 'TG_hit'
                            completed_orders.append(position)
                            positions_to_remove.append(order_id)

                    except Exception as e:
                        print(f"   ❌ Exit error for {order_id}: {e}")
                        traceback.print_exc()
                        continue

                # Remove closed positions
                for order_id in positions_to_remove:
                    pos_symbol = orderbook[order_id]['name']
                    del orderbook[order_id]

                    symbol_still_open = any(pos['name'] == pos_symbol for pos in orderbook.values())
                    if not symbol_still_open:
                        open_symbols.discard(pos_symbol)
                        print(f"\n✅ All positions closed for {pos_symbol} - NEW ENTRIES NOW ALLOWED")

        print(f"\n⏸️ Waiting 10 seconds... (Open Positions: {len(orderbook)})\n")
        time.sleep(10)

except KeyboardInterrupt:
    print("\n\n⚠️ Bot interrupted by user (Ctrl+C)")
except Exception as e:
    print(f"\n\n❌ Fatal error: {e}")
    traceback.print_exc()
finally:
    print(f"\n🛑 BOT STOPPED")
    print(f"📊 Final Status: {len(orderbook)} open position(s)\n")

    if orderbook:
        print("Open Positions:")
        for order_id, pos in orderbook.items():
            print(f"   - {order_id}: {pos['buy_sell']} @ ₹{pos['entry_price']}")
