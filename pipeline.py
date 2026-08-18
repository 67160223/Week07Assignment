import os
import datetime
import sqlite3
import pandas as pd
from dataclasses import dataclass
from typing import List

# ==========================================
# Task 1: Pipeline Configuration & Dataclass
# ==========================================
@dataclass
class PipelineConfig:
    input_path: str
    db_path: str
    batch_list: List[str]
    error_mode: str = "quarantine"

# ==========================================
# Task 3: Database & Schema Initialization
# ==========================================
def init_db(db_path: str):
    """สร้างตารางใน SQLite Database สำหรับ Star Schema"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    # 1. Dimension Tables
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_id TEXT UNIQUE NOT NULL,
        customer_name TEXT,
        province TEXT,
        segment TEXT
    );
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_product (
        product_key INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT UNIQUE NOT NULL,
        product_name TEXT,
        category TEXT
    );
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key INTEGER PRIMARY KEY, -- รูปแบบ YYYYMMDD
        full_date TEXT UNIQUE NOT NULL,
        day INTEGER,
        month INTEGER,
        quarter INTEGER,
        year INTEGER
    );
    """)
    
    # 2. Fact Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fact_sales (
        order_id TEXT PRIMARY KEY,
        date_key INTEGER,
        customer_key INTEGER,
        product_key INTEGER,
        quantity REAL,
        unit_price REAL,
        discount_pct REAL,
        gross_amount REAL,
        net_amount REAL,
        payment_method TEXT,
        sales_channel TEXT,
        updated_at TEXT,
        FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
        FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
    );
    """)
    
    # 3. Quarantine & Logging Tables
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS quarantine (
        quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT,
        order_datetime TEXT,
        customer_id TEXT,
        product_id TEXT,
        quantity TEXT,
        unit_price TEXT,
        discount_pct TEXT,
        payment_method TEXT,
        sales_channel TEXT,
        updated_at TEXT,
        source_batch TEXT,
        reason_code TEXT,
        quarantined_at TEXT
    );
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pipeline_run_log (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch TEXT,
        started_at TEXT,
        ended_at TEXT,
        rows_read INTEGER,
        rows_valid INTEGER,
        rows_rejected INTEGER,
        rows_duplicated INTEGER,
        rows_loaded INTEGER,
        status TEXT
    );
    """)
    
    conn.commit()
    conn.close()

def load_dimensions(config: PipelineConfig):
    """โหลดข้อมูล Dimension (Customers & Products) เข้า Database"""
    conn = sqlite3.connect(config.db_path)
    
    # Load Customer Dimension
    cust_df = pd.read_excel(config.input_path, sheet_name="customers").drop_duplicates(subset=["customer_id"])
    for _, row in cust_df.iterrows():
        conn.execute("""
            INSERT OR IGNORE INTO dim_customer (customer_id, customer_name, province, segment)
            VALUES (?, ?, ?, ?)
        """, (str(row["customer_id"]).strip(), row["customer_name"], row["province"], row["segment"]))
        
    # Load Product Dimension
    prod_df = pd.read_excel(config.input_path, sheet_name="products").drop_duplicates(subset=["product_id"])
    for _, row in prod_df.iterrows():
        conn.execute("""
            INSERT OR IGNORE INTO dim_product (product_id, product_name, category)
            VALUES (?, ?, ?)
        """, (str(row["product_id"]).strip(), row["product_name"], row["category"]))
        
    conn.commit()
    conn.close()

# Helper ฟังก์ชันจัดการ dim_date แบบ Dynamic
def get_or_create_date_key(conn, dt_str):
    if pd.isna(dt_str):
        return None
    dt = pd.to_datetime(dt_str, errors='coerce')
    if pd.isna(dt):
        return None
    
    date_key = int(dt.strftime("%Y%m%d"))
    full_date = dt.strftime("%Y-%m-%d")
    
    cursor = conn.cursor()
    cursor.execute("SELECT date_key FROM dim_date WHERE date_key = ?", (date_key,))
    row = cursor.fetchone()
    if row:
        return row[0]
    
    quarter = (dt.month - 1) // 3 + 1
    cursor.execute("""
        INSERT INTO dim_date (date_key, full_date, day, month, quarter, year)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (date_key, full_date, dt.day, dt.month, quarter, dt.year))
    return date_key

# ==========================================
# Task 2: Data Normalization
# ==========================================
def normalize_payment(val):
    if pd.isna(val): return None
    s = str(val).strip().lower()
    if "card" in s or "credit" in s: return "Credit Card"
    if "cash" in s: return "Cash"
    if "bank" in s or "transfer" in s or "mobile" in s or "qr" in s: return "Bank Transfer"
    return "Other"

def normalize_channel(val):
    if pd.isna(val): return None
    s = str(val).strip().lower()
    if "online" in s or "web" in s or "app" in s: return "Online"
    if "store" in s or "shop" in s or "retail" in s: return "Store"
    return "Other"

# ==========================================================
# Task 2, 3, 4: Extract, Validate, Transform, Upsert Batch
# ==========================================================
def process_batch(config: PipelineConfig, batch_name: str):
    started_at = datetime.datetime.now().isoformat()
    conn = sqlite3.connect(config.db_path)
    
    # Extract
    df_raw = pd.read_excel(config.input_path, sheet_name=batch_name)
    rows_read = len(df_raw)
    
    # ดึง Master Keys ที่มีอยู่จริงใน Dimension ออกมาเช็ค Referential Integrity
    valid_customers = set(pd.read_sql("SELECT customer_id FROM dim_customer", conn)["customer_id"])
    valid_products = set(pd.read_sql("SELECT product_id FROM dim_product", conn)["product_id"])
    
    quarantine_list = []
    valid_list = []
    
    # Data Quality Validation
    for _, row in df_raw.iterrows():
        reasons = []
        
        # 1. Check Primary Key
        order_id = str(row["order_id"]).strip() if pd.notna(row["order_id"]) else None
        if not order_id or order_id == "nan":
            reasons.append("INVALID_ORDER_ID")
            
        # 2. Check Foreign Keys
        c_id = str(row["customer_id"]).strip() if pd.notna(row["customer_id"]) else None
        if not c_id or c_id not in valid_customers:
            reasons.append("INVALID_CUSTOMER_ID")
            
        p_id = str(row["product_id"]).strip() if pd.notna(row["product_id"]) else None
        if not p_id or p_id not in valid_products:
            reasons.append("INVALID_PRODUCT_ID")
            
        # 3. Check Numeric Range & Types
        qty = pd.to_numeric(row["quantity"], errors='coerce')
        if pd.isna(qty) or qty <= 0:
            reasons.append("INVALID_QUANTITY")
            
        price = pd.to_numeric(row["unit_price"], errors='coerce')
        if pd.isna(price) or price <= 0:
            reasons.append("INVALID_UNIT_PRICE")
            
        disc = pd.to_numeric(row["discount_pct"], errors='coerce')
        if pd.isna(disc) or disc < 0 or disc > 100:
            reasons.append("INVALID_DISCOUNT_PCT")
            
        # 4. Check Datetime
        o_dt = pd.to_datetime(row["order_datetime"], errors='coerce')
        if pd.isna(o_dt):
            reasons.append("INVALID_ORDER_DATETIME")
            
        up_dt = pd.to_datetime(row["updated_at"], errors='coerce')
        if pd.isna(up_dt):
            reasons.append("INVALID_UPDATED_AT")
            
        # คัดแยก Quarantine vs Valid
        if reasons:
            q_item = row.to_dict()
            q_item["reason_code"] = "|".join(reasons)
            q_item["quarantined_at"] = datetime.datetime.now().isoformat()
            quarantine_list.append(q_item)
        else:
            v_item = row.to_dict()
            gross = qty * price
            net = gross * (1 - disc / 100.0)
            
            v_item.update({
                "order_id": order_id,
                "customer_id": c_id,
                "product_id": p_id,
                "quantity": qty,
                "unit_price": price,
                "discount_pct": disc,
                "order_datetime": o_dt,
                "updated_at": up_dt,
                "payment_method": normalize_payment(row["payment_method"]),
                "sales_channel": normalize_channel(row["sales_channel"]),
                "gross_amount": gross,
                "net_amount": net
            })
            valid_list.append(v_item)
            
    rows_rejected = len(quarantine_list)
    rows_valid = len(valid_list)
    
    # เขียน Quarantine Data ลงในตาราง quarantine
    if quarantine_list:
        pd.DataFrame(quarantine_list).to_sql("quarantine", conn, if_exists="append", index=False)
        
    rows_duplicated = 0
    rows_loaded = 0
    
    # Deduplicate & Incremental Load (Upsert)
    if valid_list:
        df_v = pd.DataFrame(valid_list)
        # Deduplicate ภายใน Batch เดียวกัน โดยเอา updated_at ล่าสุด
        df_v = df_v.sort_values(by=["order_id", "updated_at"]).groupby("order_id").last().reset_index()
        rows_duplicated = rows_valid - len(df_v)
        
        cust_map = dict(conn.execute("SELECT customer_id, customer_key FROM dim_customer").fetchall())
        prod_map = dict(conn.execute("SELECT product_id, product_key FROM dim_product").fetchall())
        
        cursor = conn.cursor()
        for _, r in df_v.iterrows():
            d_key = get_or_create_date_key(conn, r["order_datetime"])
            c_key = cust_map[r["customer_id"]]
            p_key = prod_map[r["product_id"]]
            up_str = r["updated_at"].strftime("%Y-%m-%d %H:%M:%S")
            
            # ตรวจสอบว่า order_id นี้มีอยู่ใน Fact แล้วหรือไม่ (Idempotency)
            cursor.execute("SELECT updated_at FROM fact_sales WHERE order_id = ?", (r["order_id"],))
            existing = cursor.fetchone()
            
            if existing is None:
                cursor.execute("""
                    INSERT INTO fact_sales (order_id, date_key, customer_key, product_key, quantity, unit_price, discount_pct, gross_amount, net_amount, payment_method, sales_channel, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (r["order_id"], d_key, c_key, p_key, r["quantity"], r["unit_price"], r["discount_pct"], r["gross_amount"], r["net_amount"], r["payment_method"], r["sales_channel"], up_str))
                rows_loaded += 1
            else:
                # Update ข้อมูลเฉพาะกรณีที่มี updated_at ใหม่กว่า
                if up_str > existing[0]:
                    cursor.execute("""
                        UPDATE fact_sales SET
                            date_key = ?, customer_key = ?, product_key = ?, quantity = ?, unit_price = ?,
                            discount_pct = ?, gross_amount = ?, net_amount = ?, payment_method = ?, sales_channel = ?, updated_at = ?
                        WHERE order_id = ?
                    """, (d_key, c_key, p_key, r["quantity"], r["unit_price"], r["discount_pct"], r["gross_amount"], r["net_amount"], r["payment_method"], r["sales_channel"], up_str, r["order_id"]))
                    rows_loaded += 1
                    
    ended_at = datetime.datetime.now().isoformat()
    
    # บันทึก Run Log
    conn.execute("""
        INSERT INTO pipeline_run_log (batch, started_at, ended_at, rows_read, rows_valid, rows_rejected, rows_duplicated, rows_loaded, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (batch_name, started_at, ended_at, rows_read, rows_valid, rows_rejected, rows_duplicated, rows_loaded, "SUCCESS"))
    
    conn.commit()
    conn.close()

# ==========================================
# Task 5: Orchestration Function
# ==========================================
def run_pipeline(config: PipelineConfig):
    init_db(config.db_path)
    load_dimensions(config)
    for batch in config.batch_list:
        process_batch(config, batch)

# ==========================================
# Main Execution Entry Point
# ==========================================
if __name__ == "__main__":
    # กำหนดค่า Config
    cfg = PipelineConfig(
        input_path="Python_Data_Pipeline_Lab_Dataset (1).xlsx",
        db_path="retail_dw.db",
        batch_list=["orders_batch_1", "orders_batch_2", "orders_batch_3"]
    )
    
    # รันตามลำดับการทดสอบ (4 รอบ): Batch 1 -> Batch 1 (ซ้ำ) -> Batch 2 -> Batch 3
    init_db(cfg.db_path)
    load_dimensions(cfg)
    
    print("Running Batch 1...")
    process_batch(cfg, "orders_batch_1")
    
    print("Running Batch 1 (Re-run for Idempotency Test)...")
    process_batch(cfg, "orders_batch_1")
    
    print("Running Batch 2...")
    process_batch(cfg, "orders_batch_2")
    
    print("Running Batch 3...")
    process_batch(cfg, "orders_batch_3")
    
    print("\nPipeline execution complete! SQLite DB and tables generated.")