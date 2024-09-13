import sqlite3
from datetime import datetime

today_str = datetime.today().strftime('%Y-%m-%d')
db_name = f"{today_str}-Phase1.db"
conn = sqlite3.connect(db_name)
cursor = conn.cursor()