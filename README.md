# Big Data Structure - DIA1(Group-E)


## 🎯 Project Overview

This project implements a **NoSQL database simulator** to study the impact of **data model denormalization** on:
- Storage size (GB/TB/PB)
- Sharding efficiency (over 1,000 servers)
- Query performance & costs (time, carbon footprint, monetary price)

We simulate **5 different denormalized designs** (DB1 to DB5) for an online store with:
- 10 million clients
- 100,000 products
- 4.1 billion order lines
- Average 41,000 order lines per product

The simulator is built in Python and fully automates size calculation, sharding simulation, filter/join/aggregate queries, and final model selection.

**Final conclusion (Chapter 5)**:  
**DB1** is the best model — lowest total cost, balanced sharding, realistic scalability, no extreme duplication.  
DB4 and DB5 are impractical due to massive storage explosion.

## 👥 Team Members

| Name                              | Role / Contribution                     |
|-----------------------------------|-----------------------------------------|
| Yasar Thajudeen                   | JSON Schema design & testing            |
| Luthfi Juneeda Shaj               | Size computation & sharding logic       |
| Sethulakshmi Kochuchirayil Babu   | Query simulation & cost estimation      |
| Man Vijaybai Patel                | Integration, testing & documentation    |

## 📊 Key Statistics

| Entity        | Count          | Notes                                      |
|---------------|----------------|--------------------------------------------|
| Clients       | 10,000,000     | Each makes ~100 orders                     |
| Products      | 100,000        | 5,000 brands, avg 2 categories             |
| Order Lines   | 4,100,000,000  | Avg 41,000 lines per product               |
| Warehouses    | 200            |                                            |
| Stock entries | 20,000,000     | Even distribution across warehouses        |

## 📏 Field Size Assumptions 

| Type          | Size (bytes) | Overhead per field |
|---------------|--------------|--------------------|
| Integer/Number| 8            | +12                |
| String        | 80           | +12                |
| Date          | 20           | +12                |
| Long String   | 200          | +12                |

## 🗂️ Project Structure

```text
BigDataStructure-DIA1-Group-E/
├── main.py                 # Core simulator: size calculation & sharding
├── test.py                 # Runs Chapter 2 analysis + Ch.3 & Ch.4 demos
├── query_sim.py            # Chapter 3: Filter & Join queries + costs
├── aggregate_sim.py        # Chapter 4: Aggregate queries + costs
├── run_final.py            # Chapter 5: Full challenge – all queries on 5 models
├── README.md               # This file – full project documentation
├── stats.json              # Real statistics (cardinality, avg, distinct)
└── schemas/                # 5 denormalized JSON schemas (DB1–DB5)
    ├── db1.json
    ├── db2.json
    ├── db3.json
    ├── db4.json
    └── db5.json
```

## 🚀 How to Run

1. Prerequisites
   - Python 3.8+
   - No external packages needed

2. Run Chapter 2 analysis + Ch.3 & Ch.4 demos

```
python test.py
```

3. Run final Chapter 5 challenge (full use case on all models)

```
python run_final.py
```

## 📈 Final Results

After running filter, join, and aggregate queries on all 5 models:

| Model | Total Estimated Cost | Notes / Recommendation |
| :--- | :--- | :--- |
| **DB1** | **$32.82** | **Best model** – lowest cost, balanced sharding, minimal duplication, realistic |
| **DB2** | **$32.82** | Slightly better size than DB1 but deletes Stock collection $\rightarrow$ not viable |
| **DB3** | **$32.82** | Moderate duplication (Stock contains Product) |
| **DB4** | **$32.82** | OrderLine contains Product $\rightarrow$ 4.4 TB storage $\rightarrow$ impractical |
| **DB5** | **$32.82** | Product contains ~41,000 OrderLines $\rightarrow$ ~167 PB storage $\rightarrow$ impossible |

**Winner: DB1**

Reason: Lowest overall cost, balanced data distribution across 1,000 servers, no extreme denormalization, full business functionality preserved.