import pdb
import time
import datetime
import traceback
from Dhan_Tradehull import Tradehull
import pandas as pd
from pprint import pprint
import talib
import xlwings as xw
import time
import os
from client_code_storage import client_code
from token_id_storage import token_id


client_code = client_code
token_id    = token_id
tsl         = Tradehull(client_code,token_id)


# Check if Excel file exists
excel_file = r'D:\Work\Investing\Github\Algo_trading_V1\Trade With Dhan.xlsx'

if not os.path.exists(excel_file):
    print(f"ERROR: Excel file not found at: {excel_file}")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Files in current directory:")
    for file in os.listdir(os.getcwd()):
        if file.endswith('.xlsx'):
            print(f"  - {file}")
    exit()

print(f"Excel file found: {excel_file}")

# Open Excel with explicit app instance
try:
    wb = xw.Book(excel_file)
    sheet = wb.sheets['Live_Trading']
    print("Excel workbook opened successfully")
except Exception as e:
    print(f"Error opening Excel: {e}")
    print("Trying alternative method...")
    # Alternative method: Create app instance explicitly
    app = xw.App(visible=False)
    wb = app.books.open(excel_file)
    sheet = wb.sheets['Live_Trading']
    print("Excel workbook opened successfully (alternative method)")


bot_token           = "8549724310:AAHOJhoxbl2NPzHblsi04cRVabjREadq-UU"
receiver_chat_ids   = ["6193962152", "1169187573"]
message             = [
    {
        "entry_order_id": "482512266663",
        "entry_price": 14085.0,
        "date": "2025-12-26",
        "entry_time": "10:30:17",
        "sl_order_id": "482512266673",
        "target_price": 14113.2,
        "stop_loss_price": 14056.8,
        "symbol": "GOLDPETAL JAN FUT",
        "scan_symbol": "GOLD FEB FUT",
        "quantity": 1
    },
    {
        "entry_order_id": "482512266663",
        "entry_price": 14085.0,
        "date": "2025-12-26",
        "entry_time": "10:30:17",
        "sl_order_id": "482512266673",
        "exit_price": 14112.0,
        "exit_time": "12:45:46",
        "pnl": 27.0,
        "remark": "TG_hit",
        "symbol": "GOLDPETAL JAN FUT"
    }
]


# Convert message list to formatted string
formatted_message = ""
for i, trade in enumerate(message, 1):
    formatted_message += f"📊 Trade Update #{i}\n"
    formatted_message += f"━━━━━━━━━━━━━━━━━━━━\n"
    formatted_message += f"📌 Symbol: {trade['symbol']}\n"
    formatted_message += f"📅 Date: {trade['date']}\n"
    formatted_message += f"🔢 Entry Price: ₹{trade['entry_price']}\n"
    formatted_message += f"⏰ Entry Time: {trade['entry_time']}\n"
    
    if 'exit_price' in trade:
        formatted_message += f"🎯 Exit Price: ₹{trade['exit_price']}\n"
        formatted_message += f"⏰ Exit Time: {trade['exit_time']}\n"
        formatted_message += f"💰 P&L: ₹{trade['pnl']}\n"
        formatted_message += f"📝 Remark: {trade['remark']}\n"
    else:
        formatted_message += f"🎯 Target: ₹{trade['target_price']}\n"
        formatted_message += f"🛑 Stop Loss: ₹{trade['stop_loss_price']}\n"
        formatted_message += f"📊 Quantity: {trade['quantity']}\n"
    
    formatted_message += "\n"


# Send the formatted message to both receivers
print("Sending Telegram messages...")
for chat_id in receiver_chat_ids:
    tsl.send_telegram_alert(message=formatted_message, receiver_chat_id=chat_id, bot_token=bot_token)
    print(f"Message sent to chat_id: {chat_id}")

print("All messages sent successfully!")
