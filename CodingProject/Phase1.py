#
# Author: Jacob Rydecki
# 
# Source code for Phase 1 of CS 589 Coding Project
# 
import sqlite3
import argparse
from db_phase1 import conn, cursor


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
            print(query)
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

def get_last_20_points(stock):
    query = f"SELECT MAX(rowid) FROM `{stock}`"
    max_id = cursor.execute(query).fetchall()[0][0]
    if max_id is None:
        return []
    
    query = f"SELECT * FROM `{stock}` WHERE rowid > ? AND rowid <= ?"
    rows = cursor.execute(query, (max_id-20, max_id)).fetchall()
    
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
        insert_csv("data/Sample1MinuteData.csv")

    seen_stocks = set()

    time = 400
    while time <= 1959:
        query = "SELECT * FROM stock_data WHERE Time == ?"
        rows = cursor.execute(query, (time,)).fetchall()
        for cur_row in rows:
            
            stock = cur_row[0]
            if not stock in seen_stocks:
                create_stock_db_table(stock)
                seen_stocks.add(stock)

            last_20 = get_last_20_points(stock)
            
            # Calculate SMA
            if len(last_20) >= 20:
                sum = 0
                for point in last_20:
                    sum += point[5]

                sum += cur_row[5]
                sma = sum / 21

            else:
                sma = 0

            # Calculate RangeRatio

            query = f"INSERT INTO `{stock}` VALUES (?,?,?,?,?,?,?,?)"  
            args = (*(cur_row[:-1]), sma, 0)      
            cursor.execute(query, args)
            conn.commit()

            break
            
        break
            


        time += 1





if __name__ == "__main__":
    main()
