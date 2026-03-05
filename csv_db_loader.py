import csv
import oracledb
import os
from dotenv import load_dotenv

load_dotenv()

def load_bronze():
    conn_params = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "dsn": os.getenv("DB_DSN")
    }
    
    try:
        conn = oracledb.connect(**conn_params)
        cursor = conn.cursor()

        # Truncate to keep Raw layer fresh
        cursor.execute("TRUNCATE TABLE bronze_sales_raw")

        with open('daily_sales.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip Header
            rows = [row for row in reader]
            
            # High-performance batch insert
            cursor.executemany("INSERT INTO bronze_sales_raw VALUES (:1, :2, :3, :4)", rows)

        conn.commit()
        print(f"Bronze Load: {len(rows)} records inserted.")

    except Exception as e:
        print(f"Error in Bronze Layer: {e}")
        raise e # Pass error to main.py
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    load_bronze()