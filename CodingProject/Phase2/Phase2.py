
from keys import API_KEY

import sqlite3
import threading
import schedule 
from polygon import RESTClient
from polygon.rest import models
from datetime import datetime
from functools import partial

db_lock = threading.Lock()
sma_lock = threading.Lock()
rr_lock = threading.Lock()
sma_filename = ""
rr_filename = ""
last_time = {} # Dict to track last seen timestamp for each stock 
DURATION = 30 # Minutes

def get_last21(stock, cursor):
    query = f"SELECT MAX(rowid) FROM `{stock}`"
    max_id = cursor.execute(query).fetchall()[0][0]
    if max_id is None:
        return []
    query = f"SELECT Close, High, Low FROM `{stock}` WHERE rowid > ? AND rowid <= ?"
    rows = cursor.execute(query, (max_id-21, max_id)).fetchall()
    return rows

def calc_sma(last_20, cur_close):
    if len(last_20) >= 20:
        sum = 0
        for point in last_20:
            sum += point[0]

        sum += cur_close # Add current stock close value
        sma = sum / 21
        if sma >= 1:
            sma = round(sma, 2)
        else:
            sma = round(sma, 3)
    else:
        sma = 0
    
    return sma

def calc_rr(last_21, cur_high, cur_low):
    if len(last_21) >= 21:
        max_high = max([row[1] for row in last_21])
        min_low = min([row[2] for row in last_21])
        if (max_high == min_low):
            rr = 1 # Unsure if this is the correct value
        else:
            rr = ( cur_high - cur_low ) / ( max_high - min_low )

            if rr >= 1:
                rr = round(rr, 2)
            else:
                rr = round(rr, 3)
    else:
        rr = 0

    return rr

def process_stocks(stock_list):
    global conn
    cursor = conn.cursor()

    client = RESTClient(api_key=API_KEY)
    resp = client.get_snapshot_all("stocks", stock_list)
    for snap in resp:
        time_str = datetime.fromtimestamp(snap.min.timestamp / 1000).strftime('%H:%M:%S')

        # Ignore any older datapoints
        if last_time[snap.ticker] == time_str:
            continue
        else:
            last_time[snap.ticker] = time_str

        # Get Database Rows
        with db_lock:
            last_21 = get_last21(snap.ticker, cursor)
        last_20 = last_21[:-1]

        # Calculations
        sma = calc_sma(last_20, snap.min.close)
        rr = calc_rr(last_21, snap.min.high, snap.min.low)

        # Write to Files
        with sma_lock:
            with open(sma_filename, "a") as f:
                f.write(f"{time_str} {snap.ticker} 21 SMA: {sma}\n")
        with rr_lock:
            with open(rr_filename, "a") as f:
                f.write(f"{time_str} {snap.ticker} 21 Range Ratio: {rr}\n")

        # Write Changes to DB
        with db_lock:
            query = f"INSERT INTO `{snap.ticker}` VALUES (?,?,?,?,?,?,?,?)"
            args = (snap.ticker, 
                    time_str,
                    snap.min.open,
                    snap.min.close,
                    snap.min.high,
                    snap.min.low,
                    sma,
                    rr,
                )
            cursor.execute(query, args)
            conn.commit()


def create_and_start_threads(stock_list, THREADS, interval):
    # Create Threads
    threads = []
    for i in range(THREADS):
        beg = i * interval
        end = (i + 1) * interval
        if i == (THREADS - 1): # If last interval, pass whatever is left
            threads.append(threading.Thread(target=process_stocks, args=(stock_list[beg:],)))
        else:
            threads.append(threading.Thread(target=process_stocks, args=(stock_list[beg:end],)))

    # Start Threads
    for thread in threads:
        thread.start()

    # Join Threads
    for thread in threads:
        thread.join()


if __name__ == "__main__":

    # File Variables
    today_str = datetime.today().strftime('%Y-%m-%d')
    sma_filename = f"{today_str}-SMA.txt"
    rr_filename = f"{today_str}-RangeRatio.txt"

    # DB Variables
    global conn
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    cursor = conn.cursor()

    # Get Tickers from StockList File
    stock_file = "./StockList.txt" 
    stock_list = []
    with open(stock_file, "r") as f:
        for line in f.readlines():
            stock_list.append(line.strip())
            last_time[line.strip()] = None
    num_stocks = len(stock_list)
        

    # Create Stock Tables
    print("Creating Stock Tables...")
    for ticker in stock_list:
        query = f"CREATE TABLE IF NOT EXISTS `{ticker}` (Ticker TEXT, Time TIME, Open REAL, Close REAL, High REAL, Low REAL, SMA21 REAL, RR21 REAL)"
        cursor.execute(query)
        conn.commit()
    import time
    time.sleep(1)
    
    # Run Threads
    THREADS = 6
    interval = num_stocks // THREADS
    print("Entering Processing Loop...")
    for i in range(DURATION):
        start_time = time.time()
        create_and_start_threads(stock_list, THREADS, interval)
        run_time = time.time() - start_time
        print(f"Current Minute Runtime: {run_time:.3f} seconds")
        if i != (DURATION - 1):
            time.sleep(60 - run_time)


    # Writing In-Memory to a File: https://stackoverflow.com/a/59274634
    print("Done. Writing Database File")
    db_file = sqlite3.connect(f"{today_str}-Phase1.db") 
    conn.backup(db_file)
    db_file.close()
    conn.close()
