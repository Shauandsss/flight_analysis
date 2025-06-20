# 🧱 Medallion Architecture Pipeline on Databricks

This repository contains a full implementation of a **data pipeline using the Medallion Architecture** on **Databricks**, built to demonstrate best practices for organizing and processing data through Bronze, Silver, and Gold layers.

## 📌 About the Project

This project shows how to transform a **data swamp** into a clean, scalable, and production-ready **data platform** using:

- ✅ Delta Lake
- ✅ PySpark notebooks
- ✅ Databricks Workflows
- ✅ Clear separation of technical and business logic

All layers follow the Medallion pattern:
- **Bronze:** Raw data ingestion (CSV, JSON, API, Streaming)
- **Silver:** Cleansing, casting, enrichment (split into `silver_db` and `silver_business`)
- **Gold:** Aggregation, KPIs, and datasets ready for BI or ML

## 📂 Project Structure

- `src/bronze_flights.py` — Ingests raw CSV into Bronze layer  
- `src/bronze_us_states.py` — Ingests US States dimension  
- `src/silver_flights.py` — Cleans and transforms to silver_db  
- `src/silver_business.py` — Applies business logic to create silver_business  
- `src/gold_summary.py` — Generates aggregated Gold tables  

## 🔄 Pipeline Orchestration

All notebooks are executed as tasks in a **Databricks Workflow**, with dependencies set between them. The pipeline is fully automated and can be scheduled.

## 📊 Data & Visualization

The dataset used is [US Airline Flight Routes and Fares 1993–2024](https://www.kaggle.com/datasets/bhavikjikadara/us-airline-flight-routes-and-fares-1993-2024) + US States metadata.  
Dashboards are created directly inside Databricks notebooks and can also be connected to **Power BI**, **Tableau**, or other BI tools via JDBC.

## 🚀 Getting Started

1. Clone the repository and connect it to your Databricks Workspace
2. Set up catalogs and schemas for:
   - `bronze`
   - `silver_db`
   - `silver_business`
   - `gold`
3. Run each notebook step-by-step or orchestrate them with a **Databricks Workflow**
4. Explore the final Gold tables or connect to your BI tool of choice

## 🧠 Key Concepts

- Medallion Architecture
- Delta Lake
- Databricks Workflow orchestration
- Bronze → Silver → Gold pipeline modeling
- Data cleaning and enrichment
- Streaming ingestion with Auto Loader

## 📎 Related Article

📖 *Stop Creating Data Swamps: Build a Clean Data Pipeline with Medallion Architecture*  
→ [Read on Medium](https://medium.com/@shauandss/stop-creating-data-swamps-build-a-clean-data-pipeline-with-medallion-architecture-3d6d2c1942b6)
