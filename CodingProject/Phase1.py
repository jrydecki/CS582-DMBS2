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


def initialize_tables(filename):
    stocks = []
    with open(filename, "r") as f:
        for stock in f.readlines():
            create_stock_db_table(stock.strip())
            stocks.append(stock.strip())

    


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", 
                         "--insert", 
                         default=False, 
                         action=argparse.BooleanOptionalAction, 
                         help="Dictates if the database needs to be populated -- if the CSV needs to be 'inserted'."
                        )
    args = parser.parse_args()

    if args.insert:
        insert_csv("data/Sample1MinuteData.csv")

    seen_stocks = set()

    time = 400
    while time <= 1959:
        query = "SELECT * FROM stock_data WHERE Time == ?"
        rows = cursor.execute(query, (time,))
        for row in rows:
            pass


        time += 1





if __name__ == "__main__":
    main()
