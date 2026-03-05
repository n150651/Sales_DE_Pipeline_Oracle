# Sales_DE_Pipeline_Oracle
Data Engineering project with ELT pipeline with medallion architecture following SCDTYPE 2 
# 🚀 Sales Data Pipeline: Medallion Architecture (Oracle + Python)

A professional-grade Data Engineering pipeline that moves sales data from raw CSV files into an Oracle Database using the **Medallion Architecture** (Bronze, Silver, Gold layers) and **Slowly Changing Dimensions (SCD Type 2)** for historical tracking.

## 🏗️ Architecture Overview
This project follows the industry-standard Medallion Architecture to ensure data quality and reliability:

1.  **Bronze (Raw):** Direct ingestion from CSV using high-performance `executemany` batch loading.
2.  **Silver (Clean):** Implements **SCD Type 2** logic to track historical changes and applies business rules (e.g., normalizing `Hyderabad` to `SOUTH`).
3.  **Gold (Analytical):** Provides aggregated views for business reporting and performance metrics.



## 🛠️ Tech Stack
* **Language:** Python 3.12
* **Database:** Oracle Database (XE)
* **Driver:** `python-oracledb` (Thin Mode)
* **Environment:** Python-dotenv for secure credential management
* **Version Control:** Git & GitHub

## 📈 Key Features & Business Rules
* **SCD Type 2:** Tracks price or region changes over time using `start_date`, `end_date`, and `is_current` flags.
* **Idempotency:** The pipeline can be re-run multiple times without creating duplicate records or corrupting data.
* **Data Normalization:** Automatically standardizes regional names (e.g., "Hyderabad" -> "SOUTH").
* **Deduplication:** Uses SQL Window Functions (`ROW_NUMBER()`) to handle duplicate records within source files.



## 🚀 How to Run
1.  **Clone the Repo:**
    ```bash
    git clone [https://github.com/n150651/Sales_DE_Pipeline_Oracle.git](https://github.com/n150651/Sales_DE_Pipeline_Oracle.git)
    cd Sales_DE_Pipeline_Oracle
    ```
2.  **Set up Environment Variables:**
    Create a `.env` file with your Oracle credentials:
    ```text
    DB_USER=your_user
    DB_PASSWORD=your_password
    DB_DSN=localhost:1521/xe
    ```
3.  **Initialize Database:**
    Run the `database_setup.sql` script in your Oracle SQL Developer.
4.  **Execute Pipeline:**
    ```bash
    python main.py
    ```

## ✅ Validation
The project includes a `test_pipeline.py` script to verify:
* Historical record preservation (SCD Type 2).
* Zero "Hyderabad" leakage in the Silver layer.
* Correct current flag assignment.
