# 🚀 Python Data Pipeline Engineering: Omnichannel Retail Data Warehouse

โปรเจกต์นี้เป็นการพัฒนา **ETL Pipeline (Extract, Transform, Load)** สำหรับประมวลผลข้อมูลยอดขายปลีกหลากช่องทาง (Omnichannel Retail) โดยออกแบบระบบให้รองรับการทำงานแบบ **Incremental Loading** และมีความเป็น **Idempotent** (สามารถรันซ้ำได้โดยไม่สร้างข้อมูลขยะหรือข้อมูลซ้ำซ้อน) รวมถึงมีระบบจัดการคุณภาพข้อมูล (**Data Quality & Quarantine**) และระบบบันทึกประวัติการทำงาน (**Pipeline Run Log**)

---

## 📋 สารบัญ (Table of Contents)
1. [ภาพรวมของสถาปัตยกรรม (Architecture & Star Schema)](#-ภาพรวมของสถาปัตยกรรม-architecture--star-schema)
2. [คุณสมบัติเด่นของ Pipeline (Key Features)](#-คุณสมบัติเด่นของ-pipeline-key-features)
3. [ความต้องการของระบบและการติดตั้ง (Prerequisites & Installation)](#-ความต้องการของระบบและการติดตั้ง-prerequisites--installation)
4. [วิธีการรัน Pipeline (Usage)](#-วิธีการรัน-pipeline-usage)
5. [โครงสร้างตารางข้อมูล (Database Schema)](#-โครงสร้างตารางข้อมูล-database-schema)
6. [กฎการตรวจสอบคุณภาพข้อมูล (Data Quality Rules)](#-กฎการตรวจสอบคุณภาพข้อมูล-data-quality-rules)
7. [การวิเคราะห์ Reflection](#-การวิเคราะห์-reflection)

---

## 📐 ภาพรวมของสถาปัตยกรรม (Architecture & Star Schema)

Pipeline ถูกออกแบบตามสถาปัตยกรรม **Star Schema** เพื่อรองรับการนำข้อมูลไปวิเคราะห์ต่อ (Data Analytics / BI) โดยกำหนด Grain ของ `fact_sales` เป็น **"หนึ่งรายการขายสินค้าที่ผ่านการตรวจสอบต่อ order_id"**

``text
                  +-------------------+
                  |   dim_customer    |
                  +-------------------+
                  | PK  customer_key  |
                  |     customer_id   |
                  |     customer_name |
                  |     province      |
                  |     segment       |
                  +---------+---------+
                            |
                            | 1:N
                            v
+-------------------+     +-------------------+     +-------------------+
|     dim_date      |     |    fact_sales     |     |    dim_product    |
+-------------------+     +-------------------+     +-------------------+
| PK  date_key      |<---+| PK  order_id      |+--->| PK  product_key   |
|     full_date     | 1:N | FK  date_key      | 1:N |     product_id    |
|     day           |     | FK  customer_key  |     |     product_name  |
|     month         |     | FK  product_key   |     |     category      |
|     quarter       |     |     quantity      |     +-------------------+
|     year          |     |     unit_price    |
+-------------------+     |     discount_pct  |
                          |     gross_amount  |
                          |     net_amount    |
                          |     payment_method|
                          |     sales_channel |
                          |     updated_at    |
                          +-------------------+

# ✨ คุณสมบัติเด่นของ Pipeline (Key Features)

1. **Idempotency**: สามารถรัน Batch เดิมซ้ำกี่ครั้งก็ได้ ข้อมูลใน `fact_sales` จะไม่เพิ่มขึ้นและไม่เกิดการบันทึกซ้ำ
2. **Incremental Loading & Upsert**: สามารถประมวลผลข้อมูลใหม่หรือข้อมูลอัปเดต โดยเทียบค่า `updated_at` หากพบข้อมูลที่มี `order_id` เดิมแต่มี timestamp ใหม่กว่า จะทำ Update ข้อมูลเดิมทันที
3. **Data Quality Control & Quarantine**: ข้อมูลที่ไม่ผ่าน Validation Rules (เช่น ค่าว่าง, ตัวเลขติดลบ, รหัสอ้างอิงไม่อยู่ใน Master, วันที่ผิดฟอร์แมต) จะถูกส่งไปกักเก็บในตาราง `quarantine` พร้อมระบุ `reason_code`
4. **Data Normalization**: แปลงรูปแบบ `payment_method` และ `sales_channel` ให้อยู่ในมาตรฐานเดียวกัน
5. **Run Logging**: บันทึกสถิติการประมวลผลทุก Batch ลงตาราง `pipeline_run_log` ได้แก่ จำนวนแถวที่อ่าน, Valid, Rejected, Duplicated และ Loaded

---

## 🛠 ความต้องการของระบบและการติดตั้ง (Prerequisites & Installation)

### Requirements
* **Python**: Version 3.8 หรือสูงกว่า
* **Libraries**: `pandas`, `openpyxl` (สำหรับอ่านไฟล์ Excel), `sqlite3` (Built-in)

### ขั้นตอนการติดตั้ง
1. Clone หรือดาวน์โหลดโปรเจกต์ลงในเครื่อง
2. ติดตั้ง Dependencies ที่จำเป็น:
   ```bash
   pip install pandas openpyxl
3. วางไฟล์ข้อมูลต้นทางไว้ในโฟลเดอร์เดียวกับสคริปต์ (เช่น `Python_Data_Pipeline_Lab_Dataset (1).xlsx`)

---

## 🚀 วิธีการรัน Pipeline (Usage)

รันสคริปต์หลักผ่านคำสั่ง Terminal / Command Prompt:


python pipeline.py
## 📊 โครงสร้างตารางข้อมูล (Database Schema)

### 1. Dimension Tables
* **`dim_customer`**: เก็บข้อมูลลูกค้า (`customer_key` [PK], `customer_id` [Unique], `customer_name`, `province`, `segment`)
* **`dim_product`**: เก็บข้อมูลสินค้า (`product_key` [PK], `product_id` [Unique], `product_name`, `category`)
* **`dim_date`**: เก็บมิติของเวลา (`date_key` [PK: YYYYMMDD], `full_date`, `day`, `month`, `quarter`, `year`)

### 2. Fact Table
* **`fact_sales`**: เก็บรายการธุรกรรมการขาย (`order_id` [PK], `date_key` [FK], `customer_key` [FK], `product_key` [FK], `quantity`, `unit_price`, `discount_pct`, `gross_amount`, `net_amount`, `payment_method`, `sales_channel`, `updated_at`)

### 3. Management & DQ Tables
* **`quarantine`**: เก็บข้อมูลที่ไม่ผ่าน Data Quality Check
* **`pipeline_run_log`**: บันทึกประวัติการประมวลผลของแต่ละ Batch

---

## 🔍 กฎการตรวจสอบคุณภาพข้อมูล (Data Quality Rules)

รายการที่ไม่ผ่านเกณฑ์จะถูกแยกไปยังตาราง `quarantine` พร้อม `reason_code` ดังนี้:

| กฎการตรวจสอบ (Rule) | เงื่อนไขการตรวจสอบ | Reason Code |
| :--- | :--- | :--- |
| **Primary Key** | `order_id` ห้ามเป็นค่าว่าง หรือ Null | `INVALID_ORDER_ID` |
| **Referential Integrity** | `customer_id` ต้องมีอยู่ใน `dim_customer` | `INVALID_CUSTOMER_ID` |
| **Referential Integrity** | `product_id` ต้องมีอยู่ใน `dim_product` | `INVALID_PRODUCT_ID` |
| **Numeric Range** | `quantity` ต้อง > 0 และเป็นตัวเลข | `INVALID_QUANTITY` |
| **Numeric Range** | `unit_price` ต้อง > 0 และเป็นตัวเลข | `INVALID_UNIT_PRICE` |
| **Numeric Range** | `discount_pct` ต้องอยู่ระหว่าง 0 - 100 | `INVALID_DISCOUNT_PCT` |
| **Datetime Valid** | วันที่และเวลาต้องอยู่ในรูปแบบถูกต้อง | `INVALID_DATETIME` |

---

## 💬 การวิเคราะห์ Reflection

### เหตุใด Availability จึงมักสำคัญกว่า Strictness ใน Production Data Pipeline?

ในการพัฒนา Production Data Pipeline การสร้างสมดุลระหว่าง **Availability (ความพร้อมใช้งานของระบบและข้อมูล)** กับ **Strictness (ความเข้มงวดในการตรวจสอบ)** เป็นสิ่งสำคัญอย่างยิ่ง ในระบบงานจริง ข้อมูลจากระบบต้นทางมักมีความไม่สมบูรณ์ (Dirty Data) เกิดขึ้นเป็นประจำ หากเราใช้แนวทางแบบ *Strictness* ที่มีความเข้มงวดสูงมาก โดยกำหนดให้ Pipeline หยุดการทำงานทันที (Fail-fast / Abort Process) เมื่อพบข้อมูลผิดปกติแม้เพียงบรรทัดเดียว จะส่งผลกระทบให้ข้อมูลส่วนใหญ่ที่ดีและถูกต้องไม่สามารถไหลเข้าสู่ Data Warehouse ได้ เกิดความล่าช้าต่อการทำรายงาน รายงาน Dashboard ผู้บริหารหยุดชัก และกระทบกระเทือนต่อธุรกิจ

ในทางกลับกัน การให้ความสำคัญกับ **Availability** โดยใช้กลยุทธ์ **Data Quarantine Framework** จะช่วยให้ Pipeline สามารถประมวลผลข้อมูลที่ดี (Clean Records) เข้าสู่ระบบปลายทางได้อย่างต่อเนื่อง ทำให้ระบบงานอื่น ๆ สามารถใช้งานข้อมูลส่วนใหญ่ได้ทันที ไม่เกิด Downtime ในขณะเดียวกัน ข้อมูลที่มีปัญหาจะถูกคัดแยกออกไปเก็บไว้ใน Quarantine Table พร้อมระบุสาเหตุอย่างชัดเจน เพื่อให้ทีม Data Engineer หรือ Data Governance เข้ามาตรวจสอบและแก้ไขภายหลัง การออกแบบลักษณะนี้ช่วยรักษาทั้ง **Continuity ของธุรกิจ** และ **Quality ของข้อมูล** ไปพร้อมกันอย่างยั่งยืน
