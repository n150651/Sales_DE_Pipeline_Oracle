import oracledb
import os
from dotenv import load_dotenv

load_dotenv()

def quarantine_bad_data(cursor):
    """Identify and remove bad data from Bronze before Silver processing."""
    
    # Use raw triple quotes to avoid backslash warnings and handle multiline SQL
    quarantine_query = r"""
    INSERT INTO silver_sales_quarantine (sale_id, raw_data, error_reason)
    SELECT 
        sale_id, 
        ('ID:' || NVL(sale_id, 'NULL') || '|DT:' || NVL(sale_date, 'NULL') || '|AMT:' || NVL(amount, 'NULL')),
        'INVALID_DATA_FORMAT'
    FROM bronze_sales_raw
    WHERE (NOT REGEXP_LIKE(amount, '^[0-9]+(\.[0-9]+)?$')) 
       OR (sale_id IS NULL)
    """
    
    delete_query = r"""
    DELETE FROM bronze_sales_raw 
    WHERE (NOT REGEXP_LIKE(amount, '^[0-9]+(\.[0-9]+)?$')) 
       OR (sale_id IS NULL)
    """

    cursor.execute(quarantine_query)
    rejected_count = cursor.rowcount

    if rejected_count > 0:
        cursor.execute(delete_query)
        print(f"⚠️ Quarantined {rejected_count} bad records.")
        
    return rejected_count

def load_silver():
    conn_params = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "dsn": os.getenv("DB_DSN")
    }

    try:
        conn = oracledb.connect(**conn_params)
        cursor = conn.cursor()

        # NEW: Data Quality Circuit Breaker
        quarantine_bad_data(cursor)

        # 1. EXPIRE LOGIC
        # This updates existing records to 'N' if the data has changed in Bronze
        expire_sql = r"""
        UPDATE silver_sales_clean s
        SET s.end_date = TRUNC(SYSDATE), 
            s.is_current = 'N', 
            s.upd_dt = SYSDATE
        WHERE s.is_current = 'Y'
        AND EXISTS (
            SELECT 1 FROM (
                SELECT TO_NUMBER(REPLACE(TRIM(sale_id), 'S', '')) as b_id, 
                       TRUNC(TO_DATE(sale_date, 'YYYY-MM-DD')) as b_dt,
                       ROUND(CAST(amount AS NUMBER), 2) as b_amt,
                       CASE WHEN UPPER(TRIM(region)) = 'HYDERABAD' THEN 'SOUTH' 
                            ELSE UPPER(TRIM(region)) END as b_reg,
                       ROW_NUMBER() OVER (PARTITION BY sale_id, sale_date ORDER BY ROWID DESC) as rnk
                FROM bronze_sales_raw
            ) b
            WHERE b.rnk = 1 
              AND b.b_id = s.sale_id 
              AND b.b_dt = TRUNC(s.sale_date)
              AND (b.b_amt <> s.amount OR b.b_reg <> s.region)
        )
        """

        # 2. INSERT LOGIC
        # This inserts new records or the "new version" of changed records
        insert_sql = r"""
        INSERT INTO silver_sales_clean (sale_id, sale_date, amount, region, start_date, end_date, is_current)
        SELECT b_id, b_dt, b_amt, b_reg, TRUNC(SYSDATE), TO_DATE('9999-12-31', 'YYYY-MM-DD'), 'Y'
        FROM (
            SELECT TO_NUMBER(REPLACE(TRIM(sale_id), 'S', '')) as b_id,
                   TRUNC(TO_DATE(sale_date, 'YYYY-MM-DD')) as b_dt,
                   ROUND(CAST(amount AS NUMBER), 2) as b_amt,
                   CASE WHEN UPPER(TRIM(region)) = 'HYDERABAD' THEN 'SOUTH' 
                        ELSE UPPER(TRIM(region)) END as b_reg,
                   ROW_NUMBER() OVER (PARTITION BY sale_id, sale_date ORDER BY ROWID DESC) as rnk
            FROM bronze_sales_raw
        ) b
        WHERE b.rnk = 1
        AND NOT EXISTS (
            SELECT 1 FROM silver_sales_clean s 
            WHERE s.sale_id = b.b_id 
              AND TRUNC(s.sale_date) = b.b_dt 
              AND s.is_current = 'Y'
              AND s.amount = b.b_amt 
              AND s.region = b.b_reg
        )
        """

        cursor.execute(expire_sql)
        cursor.execute(insert_sql)
        conn.commit()
        print("Silver Load: SCD Type 2 logic applied successfully.")

    except Exception as e:
        print(f"Error in Silver Layer: {e}")
        raise e
    finally:
        if conn:
            conn.close()