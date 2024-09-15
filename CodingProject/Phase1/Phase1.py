#
# Author: Jacob Rydecki
# 
# Source code for Phase 1 of CS 589 Coding Project
# 
import sqlite3
import argparse
import datetime
import json
from time import perf_counter_ns
from db_phase1 import conn, cursor, today_str


def create_stock_data_table():
    query = ("CREATE TABLE IF NOT EXISTS stock_data ("
             "Stock TEXT,"
             "Time INT, "
             "Open REAL, "
             "High REAL, "
             "Low REAL, "
             "Close REAL, "
             "Volume INT"
             ")"
            )
    cursor.execute(query)

def insert_csv(filename):
    create_stock_data_table()
    with open(filename, "r") as f:
        f.readline()
        for line in f.readlines():
            fields = line.strip().split(",")
            query = (f"INSERT INTO stock_data VALUES ("
                     f"'{fields[0]}', " # Stock
                     f"{fields[1]}, " # Time
                     f"{fields[2]}, " # Open
                     f"{fields[3]}, " # High
                     f"{fields[4]}, " # Low
                     f"{fields[5]}, " # Close
                     f"{fields[6]}"   # Volume
                     ")"
                    )
            cursor.execute(query)
        conn.commit()

def create_stock_db_table(stock_name):
    query = (f"CREATE TABLE IF NOT EXISTS `{stock_name}` ("
              "Stock TEXT,"
              "Time INT, "
              "Open REAL, "
              "High REAL, "
              "Low REAL, "
              "Close REAL, "
              "SMA21 REAL, "
              "RangeRatio21 REAL"
              ")"
            )
    cursor.execute(query)
    conn.commit()


def initialize_tables(filename):
    stocks = []
    with open(filename, "r") as f:
        for stock in f.readlines():
            create_stock_db_table(stock.strip())
            stocks.append(stock.strip())

def get_last_points(stock, num):
    query = f"SELECT MAX(rowid) FROM `{stock}`"
    max_id = cursor.execute(query).fetchall()[0][0]
    if max_id is None:
        return []
    
    query = f"SELECT * FROM `{stock}` WHERE rowid > ? AND rowid <= ?"
    rows = cursor.execute(query, (max_id-num, max_id)).fetchall()
    
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", 
                         "--insert", 
                         default=False, 
                         action=argparse.BooleanOptionalAction, 
                         help="Dictates if the database needs to be populated -- if the CSV needs to be 'inserted'."
                        )
    cmd_args = parser.parse_args()

    if cmd_args.insert:
        print("Creating database from CSV file...")
        insert_csv("data/Sample1MinuteData.csv")
        print("Done!")

    seen_stocks = set()

    print("Starting Processing")

    
    THING = {}
    time = 400
    while time <= 950: # 1959
        THING[time] = {
            "last_21": [],
            "sma": [],
            "rr": [],
            "insert": [],
        }
        print(f"[{datetime.datetime.now().time()}] Proccessing for Time: {time}...")
        sma_file = f"SMA/{today_str}-{time}-SMA.txt"
        rr_file = f"RR/{today_str}-{time}-RangeRatio.txt"

        query = "SELECT * FROM stock_data WHERE Time == ?"
        rows = cursor.execute(query, (time,)).fetchall()
        with open(sma_file, "a") as sma_f, open(rr_file, "a") as rr_f:
            for cur_row in rows:
                
                stock = cur_row[0]
                if not stock in seen_stocks:
                    create_stock_db_table(stock)
                    seen_stocks.add(stock)

                t1 = perf_counter_ns()
                last_21 = get_last_points(stock, 21)
                last_20 = last_21[:-1]
                t2 = perf_counter_ns()
                THING[time]["last_21"].append(t2-t1)
                
                #print(f"Last 21 Points Time: {t2-t1} ns")
                
                # Calculate SMA
                t1 = perf_counter_ns()
                if len(last_20) >= 20:
                    sum = 0
                    for point in last_20:
                        sum += point[5]

                    sum += cur_row[5] # Add current stock value
                    sma = sum / 21
                    if sma >= 1:
                        sma = round(sma, 2)
                    else:
                        sma = round(sma, 3)

                else:
                    sma = 0
                t2 = perf_counter_ns()
                THING[time]["sma"].append(t2-t1)

                # Calculate RangeRatio
                t1 = perf_counter_ns()
                if len(last_21) >= 21:
                    cur_high = cur_row[3]
                    cur_low = cur_row[4]
                    max_high = max([row[3] for row in last_21])
                    min_low = min([row[4] for row in last_21])

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
                t2 = perf_counter_ns()
                THING[time]["rr"].append(t2-t1)

                # Write Required Output
                sma_f.write(f"{time} {stock} 21 SMA: {sma}\n")
                rr_f.write(f"{time} {stock} 21 Range Ratio: {rr}\n")

                

                # Add changes to DB
                t1 = perf_counter_ns()
                query = f"INSERT INTO `{stock}` VALUES (?,?,?,?,?,?,?,?)"  
                args = (*(cur_row[:-1]), sma, rr)      
                cursor.execute(query, args)
                conn.commit()
                t2 = perf_counter_ns()
                THING[time]["insert"].append(t2-t1)
            
        time += 1


    json_string = json.dumps(THING, indent=4)
    with open("out.json", "w") as f:
        f.write(json_string)
    
    

    


if __name__ == "__main__":
    main()
