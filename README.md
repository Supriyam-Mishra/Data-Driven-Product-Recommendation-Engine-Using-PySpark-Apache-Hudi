# Data-Driven-Product-Recommendation-Engine-Using-PySpark-Apache-Hudi
# Ecommerce Top-Seller Items Recommendation System


This project implements a full end-to-end ETL + Recommendation pipeline using:

- PySpark
- Apache Hudi
- Medallion Architecture (Bronze → Silver → Gold)
- YAML-driven configuration
- Quarantine zone for bad data
- Data Quality checks for all datasets

---

## Project Components

### 1. ETL Pipelines (src/)
- etl_seller_catalog.py  
- etl_company_sales.py  
- etl_competitor_sales.py

Tasks performed:
- Data ingestion from CSV (dirty files)
- Data cleaning
- Type casting + standardization
- DQ checks (missing IDs, invalid values, bad dates, negative quantities)
- Bad data → Quarantine zone
- Clean data → Hudi tables

---

### 2. Consumption Layer (Recommendation Engine)
- consumption_recommendation.py

Tasks performed:
- Read 3 Hudi tables
- Compute top-selling items (company + competitor)
- Identify missing items per seller
- Calculate:
  - expected_units_sold
  - market_price
  - expected_revenue
- Output final CSV in Gold layer

---

## Configuration
All file paths defined in:

configs/ecomm_prod.yml


---

## Running the Pipelines

Use spark-submit scripts under `/scripts`:

./scripts/etl_seller_catalog_spark_submit.sh

./scripts/etl_company_sales_spark_submit.sh

./scripts/etl_competitor_sales_spark_submit.sh

./scripts/consumption_recommendation_spark_submit.sh


---

## Final Output
Final recommendation CSV files located at:

processed/recommendations_csv/seller_recommend_data/
