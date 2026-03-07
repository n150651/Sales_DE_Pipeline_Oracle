# 🚀 Enterprise Sales Data Pipeline: Medallion Architecture
### **Oracle Database | Python | SCD Type 2 | Data Observability**

A **production-grade ETL pipeline** designed for high-availability environments. This project automates the movement of sales data from raw CSV sources into an Oracle Database, implementing **Medallion Architecture** and **Slowly Changing Dimensions (SCD Type 2)**. 

Built with a **production-first mindset**, it features integrated **data quality gates**, **automated quarantine logic**, and **execution auditing** to support 24/7 operations.



---

## 🏗️ **System Architecture & Data Flow**

This project follows the industry-standard Medallion Architecture to ensure data integrity at every stage:

1. **Bronze (Raw Ingestion):** - High-performance batch loading from CSV using `executemany`.
   - Temporary storage for raw, unvalidated data to ensure high-speed landing.

2. **Quarantine (The "Hospital" Pattern):** - An automated **Circuit Breaker** that identifies malformed data (non-numeric amounts, null identifiers).
   - Records are moved to `SILVER_SALES_QUARANTINE` with error reasons, preventing pipeline crashes and ensuring the Silver layer remains "clean."

3. **Silver (Refined & Historical):** - **SCD Type 2 Implementation:** Maintains a full history of changes using `start_date`, `end_date`, and `is_current` flags.
   - **Business Logic:** Normalizes regional data (e.g., standardizing "Hyderabad" to "SOUTH") and deduplicates records using SQL Window Functions (`ROW_NUMBER()`).

4. **Audit & Observability:** - Every execution is logged in `PIPELINE_LOGS`, tracking status (**STARTED/SUCCESS/FAILED**), duration, and traceback messages for immediate troubleshooting.



---

## 📈 **Engineering Highlights**

* **Idempotent Design:** The pipeline can be re-run safely; logic ensures that only new or changed data is processed.
* **Performance Optimized:** Uses Oracle **Inline Views** and **Set-based SQL** operations rather than row-by-row processing.
* **Production Readiness:** Designed for **6 AM support shifts** with clear validation queries and automated error logging.



---

## 🛠️ **Tech Stack**

* **Language:** Python 3.12
* **Database:** Oracle Database (XE)
* **Library:** `python-oracledb` (Thin Mode)
* **Security:** `python-dotenv` for environment variable management.

---

## 🚀 **Getting Started**

### **1. Prerequisites**
- Oracle Database (local or cloud)
- Python 3.x installed

### **2. Setup**
Clone the repository and install dependencies:
```bash
git clone [https://github.com/n150651/Sales_DE_Pipeline_Oracle.git](https://github.com/n150651/Sales_DE_Pipeline_Oracle.git)
cd Sales_DE_Pipeline_Oracle