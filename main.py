import time
from csv_db_loader import load_bronze
from load_silver_sales import load_silver

def run_pipeline():
    start_time = time.time()
    print("🚀 Starting End-to-End Pipeline...")

    try:
        # Step 1
        load_bronze()
        
        # Step 2
        load_silver()

        end_time = time.time()
        duration = round(end_time - start_time, 2)
        print(f"\n✅ Pipeline completed in {duration} seconds.")

    except Exception:
        print("\n❌ Pipeline failed during execution.")

if __name__ == "__main__":
    run_pipeline()