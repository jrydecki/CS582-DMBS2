import sqlite3
from datetime import datetime

db_name = f"{datetime.today().strftime('%Y-%m-%d')}-Phase1.db"
conn = sqlite3.connect(db_name)
cursor = conn.cursor()