import oracledb
import os
from dotenv import load_dotenv

load_dotenv()

def test_results():
    conn_params = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "dsn": os.getenv("DB_DSN")
    }
    
    conn = oracledb.connect(**conn_params)
    cursor = conn.cursor()
    
    print("🔍 Starting Automated Data Validation...")
    
    # TEST 1: Check Hyderabad Mapping
    cursor.execute("SELECT COUNT(*) FROM silver_sales_clean WHERE region = 'HYDERABAD'")
    hyderabad_count = cursor.fetchone()[0]
    assert hyderabad_count == 0, f"❌ FAIL: Found {hyderabad_count} Hyderabad records! Mapping failed."
    print("✅ PASS: No 'HYDERABAD' records found (Mapping successful).")
    
    # TEST 2: Check SCD Type 2 logic for Sale 102
    cursor.execute("SELECT COUNT(*) FROM silver_sales_clean WHERE sale_id = 102")
    s102_count = cursor.fetchone()[0]
    assert s102_count >= 2, "❌ FAIL: SCD Type 2 failed for Sale 102. History not preserved."
    print(f"✅ PASS: Sale 102 has {s102_count} versions (History preserved).")

    # TEST 3: Check is_current Integrity
    cursor.execute("SELECT COUNT(*) FROM silver_sales_clean WHERE is_current = 'Y' AND end_date < SYSDATE")
    integrity_check = cursor.fetchone()[0]
    assert integrity_check == 0, "❌ FAIL: Found 'Current' records with past end dates."
    print("✅ PASS: All 'Active' records have valid end dates.")

    print("\n🎉 ALL TESTS PASSED! Data integrity is 100%.")
    conn.close()

if __name__ == "__main__":
    test_results()