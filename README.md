# BigDataStructure-DIA1-Group-E
# Big Data Infrastructure and Cloud  
## 🧮 TD1 – NoSQL Data Model Simulation  

This project simulates the **storage size** and **data distribution (sharding)** of different NoSQL database models.  
It is part of the *Big Data Infrastructure and Cloud* course.  

---

## 🎯 Project Description  

The goal of this team project is to explore how **data organization in NoSQL databases** impacts storage space and data distribution across servers.  
The Python tool created in this project:  
1. Loads multiple **JSON schemas** representing database designs.  
2. Uses provided **statistics** (number of clients, orders, products, etc.).  
3. Computes:  
   - Estimated document and collection sizes  
   - Total database storage in GB/TB/PB  
4. Simulates **sharding** across servers to evaluate data distribution efficiency.  

---

## 👥 Team Members  

| Name | 
|------|
| **Yasar Thajudeen** 
| **Luthfi Juneeda Shaj** 
| **Sethulakshmi Kochuchirayil Babu** 
| **Man Vijaybai Patel** 

---

## 🧩 Key Statistics Used  

| Entity | Count | Notes |
|---------|--------|--------|
| Clients | 10 million | Each makes 100 orders |
| Products | 100,000 | 5,000 brands, 2 categories avg |
| Order Lines | 4.1 billion | Balanced over 365 days |
| Warehouses | 200 |  |
| Stock | 20 million | Even for 0 quantity |

Each product links to about **41,000 order lines** on average — a major issue for denormalized designs!

---

## 🧮 Field Size Assumptions  

| Type | Bytes |
|------|--------|
| Integer / Number | 8 |
| String | 80 |
| Date | 20 |
| Long String | 200 |
| Key Overhead | 12 per field |

---

## 🗃️ Database Design Overview  

| DB Design | Description | Notes |
|------------|--------------|-------|
| **DB1** | Product with categories & supplier | Basic merge – best balance |
| **DB2** | Product with stock array | Adds stock info inside product |
| **DB3** | Stock with full product inside | High duplication of product data |
| **DB4** | OrderLine with product inside | Heavy duplication (4.1B times) |
| **DB5** | Product with order lines array | Each product has ~41k lines (massive size) |

---

## 🗂️ Project Structure  

```bash
project/
│
├── TD_1/
│   ├── main.py             # Main simulator code
│   ├── test.py             # Tests all 5 DBs and compares results
│   ├── stats.json          # Statistical data used in the simulation
│   ├── schemas/            # JSON schemas for all 5 DB designs
│   │   ├── db1.json
│   │   ├── db2.json
│   │   ├── db3.json
│   │   ├── db4.json
│   │   └── db5.json
│   └── README.md           # Internal documentation for TD1
└── README.md               # Root-level README (this file)

🚀 How to Run the Project
1️⃣ Prerequisites

Ensure you have Python 3.10+ installed on your system.

2️⃣ Run the Simulation

Open a terminal inside the TD_1/ folder and run:

python test.py

🧾 Example Output
DB1 total: 909 GB – best one!
DB4: 4.4 TB – too big!
DB5: 167 PB – impossible!
