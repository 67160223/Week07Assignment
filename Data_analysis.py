import sqlite3
import pandas as pd

# เชื่อมต่อกับไฟล์ฐานข้อมูล (เปลี่ยน filename.db เป็นชื่อไฟล์ของคุณ)
conn = sqlite3.connect("retail_dw.db")

# ดูรายชื่อตารางทั้งหมดในไฟล์
tables = pd.read_sql_query(
    "SELECT name FROM sqlite_master WHERE type='table';", conn
)
print("ตารางทั้งหมด:", tables)

# ดึงข้อมูลจากตารางยอดขาย (fact_sales) มาดูตัวอย่าง 5 แถวแรก
df_sales = pd.read_sql_query("SELECT * FROM fact_sales LIMIT 5;", conn)
print(df_sales)

# ปิดการเชื่อมต่อ
conn.close()