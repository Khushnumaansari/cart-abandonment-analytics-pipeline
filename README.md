# 🛒 Cart Abandonment Analytics Pipeline

An end-to-end data engineering and analytics project that 
automatically collects, processes, and visualizes e-commerce 
cart abandonment data using Azure cloud services.

---

## 📊 Live Dashboard

<img width="1382" height="781" alt="Dashboard_01_executive_overview" src="https://github.com/user-attachments/assets/d0bc6ce4-8079-4c1f-a9c7-76e0ee23166c" />



---

## 🏗️ Architecture

<img width="921" height="691" alt="Architecture_diagram" src="https://github.com/user-attachments/assets/d35166f8-685d-493b-a896-5e36e9f9420f" />



**Data flows automatically every hour:**
1. Python script fetches data from public API (DummyJSON)
2. Enriched JSON lands in Azure Blob Storage
3. ADF pipeline copies data into SQL staging table
4. Stored Procedure 1 upserts into main table (removes duplicates)
5. Stored Procedure 2 refreshes 4 analytics tables
6. Power BI dashboard auto-refreshes from Azure SQL

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Source | DummyJSON Public API |
| Ingestion | Python + Azure Blob Storage |
| Orchestration | Azure Data Factory (ADF) |
| Storage | Azure SQL Database |
| Transformation | T-SQL Stored Procedures |
| Visualization | Power BI |
| Scheduling | Python Schedule + ADF Hourly Trigger |

---

## 🔄 Pipeline Details

### Stage 1: Data Ingestion
- Python fetches 50 products, carts, and users from DummyJSON API
- Enriches data with business logic (abandonment probability, 
  device type, session duration)
- Uploads JSON to Azure Blob Storage every hour

### Stage 2: ADF Pipeline
- Wildcard file path picks up all new JSON files automatically
- Copy Data activity loads into staging table (dbo.cart_events)
- Stored Procedure removes duplicates via MERGE/UPSERT logic

### Stage 3: Transformation
- Main table (dbo.main_cart_events) holds clean, deduplicated data
- Four analytics tables rebuilt automatically each run:
  - CartData_Cleaned — cleaned and enriched records
  - Daily_Summary — daily KPI aggregations
  - Category_Performance — revenue by product category
  - Abandonment_Analysis — abandonment reasons breakdown

### Stage 4: Visualization
- Power BI connected directly to Azure SQL
- Dashboard shows abandonment rate, lost revenue, 
  device breakdown, category performance

---

## 💡 Key Business Insights Tracked
- Cart abandonment rate by device type
- Lost revenue by abandonment reason
- Daily revenue vs potential revenue gap
- Category-level conversion performance
- Registered vs guest user behaviour

---

## ⚙️ How to Run Locally

1. Clone this repo
2. Install dependencies: `pip install -r scripts/requirements.txt`
3. Add your Azure connection string to `.env` file
4. Run: `python scripts/hourly_data_generator.py`

> Note: Azure credentials are not included. 
> You will need your own Azure Storage and SQL accounts.

---
## Limitations & Future Improvements

- The Python generator and ADF trigger currently run independently, so production deployment should use a single orchestrated trigger.
- The pipeline should use event_id or batch_id instead of only cart_id for stronger deduplication.
- A pipeline audit table can be added to track row counts and refresh status.
- Blob files can be archived after processing to avoid scanning historical files repeatedly.
- Power BI refresh can be scheduled after SQL procedure completion.
