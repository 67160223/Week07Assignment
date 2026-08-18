import sqlite3
import pandas as pd

# 1. เชื่อมต่อฐานข้อมูล Data Warehouse
conn = sqlite3.connect("retail_dw.db")

# 2. เขียน SQL JOIN ตาราง Fact และ Dimensions เข้าด้วยกัน
sql_query = """
SELECT 
    f.order_id,
    d.full_date,
    d.year,
    d.month,
    c.customer_name,
    c.province,
    c.segment,
    p.product_name,
    p.category,
    f.quantity,
    f.unit_price,
    f.discount_pct,
    f.gross_amount,
    f.net_amount,
    f.payment_method,
    f.sales_channel
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_date d ON f.date_key = d.date_key;
"""

# 3. โหลดข้อมูลลง Pandas DataFrame
df = pd.read_sql_query(sql_query, conn)
conn.close()

# 4. ตัวอย่างการวิเคราะห์ข้อมูลยอดขาย
print("=== สรุปยอดขายรวม ===")
print(f"ยอดขายสุทธิตั้งหมด (Net Amount): {df['net_amount'].sum():,.2f} บาท")
print(f"จำนวนออเดอร์ทั้งหมด: {df['order_id'].nunique():,} รายการ\n")

print("=== ยอดขายแยกตามหมวดหมู่สินค้า (Category) ===")
sales_by_cat = df.groupby('category')['net_amount'].sum().reset_index()
print(sales_by_cat.sort_values(by='net_amount', ascending=False))

print("\n=== ยอดขายแยกตามช่องทางการขาย (Sales Channel) ===")
sales_by_channel = df.groupby('sales_channel')['net_amount'].sum().reset_index()
print(sales_by_channel)