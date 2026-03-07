import time
import os
import oracledb
from dotenv import load_dotenv
from csv_db_loader import load_bronze
from load_silver_sales import load_silver

load_dotenv()

def log_pipeline_status(status, log_id=None, error_msg=None):
    conn = oracledb.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dsn=os.getenv("DB_DSN")
    )
    cursor = conn.cursor()
    
    try:
        if status == "STARTED":
            # 1. Insert the record
            cursor.execute(
                "INSERT INTO pipeline_logs (job_name, status) VALUES ('SALES_PIPELINE', 'STARTED')"
            )
            # 2. Get the ID that was just generated (Identity column)
            cursor.execute("SELECT MAX(log_id) FROM pipeline_logs")
            new_id = cursor.fetchone()[0]
            
            conn.commit()
            return new_id
        
        else:
            # Update existing log entry
            cursor.execute(
                "UPDATE pipeline_logs SET status = :1, end_ts = CURRENT_TIMESTAMP, error_msg = :2 WHERE log_id = :3",
                [status, error_msg, log_id]
            )
            conn.commit()
    finally:
        cursor.close()
        conn.close()

def run_pipeline():
    start_time = time.time()
    print("🚀 Starting End-to-End Pipeline...")
    
    # NEW: Start the audit log
    current_log_id = log_pipeline_status("STARTED")

    try:
        # Step 1: Load Bronze
        load_bronze()
        
        # Step 2: Load Silver (SCD Type 2 + Quarantine logic)
        load_silver()

        # SUCCESS: Update log
        log_pipeline_status("SUCCESS", log_id=current_log_id)
        
        end_time = time.time()
        duration = round(end_time - start_time, 2)
        print(f"\n✅ Pipeline completed in {duration} seconds.")

    except Exception as e:
        # FAILURE: Update log with the error message
        error_str = str(e)
        log_pipeline_status("FAILED", log_id=current_log_id, error_msg=error_str)
        print(f"\n❌ Pipeline failed: {error_str}")

if __name__ == "__main__":
    run_pipeline()