# Credit Card Fraud Detection — Data Engineering Project

## Overview

An end-to-end data engineering solution built on Microsoft Fabric for 
credit card fraud analytics. The project ingests raw transactional data 
from a PostgreSQL source system, transforms it through three medallion 
architecture layers, and delivers business-ready fraud intelligence 
through a five-page Power BI dashboard.

---

## Architecture

The solution follows the Medallion Architecture pattern — three 
progressively refined data layers that separate raw ingestion, 
cleansing and business logic into clearly defined responsibilities.

- **Bronze** — Raw data lands here exactly as it left the source. 
  No transformations. Full audit trail.
- **Silver** — Data is cleansed, standardised and enriched. 
  Nulls rejected, duplicates removed, business columns derived.
- **Gold** — Star schema dimensional model optimised for analytics. 
  SCD Type 2 history tracking implemented.

![Architecture Diagram](architecture/architecture_diagram.png)

---

## Data Model

The Gold layer is built as a star schema with one central fact table 
surrounded by four conformed dimension tables.

**Fact table:**
- Fact_Transactions — grain is one transaction row

**Dimension tables:**
- Dim_Customer — SCD Type 2, tracks customer relocations and profile changes
- Dim_Card — SCD Type 2, tracks card upgrades and status changes
- Dim_Merchant — SCD Type 2, tracks merchant category and risk changes
- Dim_Date — calendar intelligence including weekend flag and quarter

![Star Schema](architecture/star_schema_model.png)

---

## Technology Stack

| Layer | Technology |
|---|---|
| Source | PostgreSQL |
| Ingestion | Microsoft Fabric Data Pipeline |
| Storage | Microsoft Fabric Lakehouse |
| Transformation | Dataflow Gen2 |
| Business logic | PySpark Notebooks |
| Analytical model | Microsoft Fabric Semantic Model |
| Reporting | Power BI |
| Data generation | Python |

---

## Pipeline Flow

1. A metadata-driven pipeline reads source table configuration 
   from a lookup activity
2. A Foreach loop dynamically iterates over each table
3. Copy activities ingest each table from PostgreSQL as CSV 
   into the Lakehouse bronze layer
4. PySpark notebooks create Delta tables in the bronze schema
5. Dataflow Gen2 applies cleansing rules and loads silver tables
6. PySpark notebooks apply SCD Type 2 logic and load the gold layer
7. Power BI connects to the semantic model via Direct Lake mode

---

## Key Features

- Medallion Architecture — Bronze, Silver, Gold layers
- SCD Type 2 — full customer, card and merchant history tracking
- Fraud risk scoring — FraudScore derived column on every transaction
- Dynamic business rules — SpendCategory, AgeGroup, CardTier, IsExpired
- Data governance — null rejection, deduplication, referential integrity
- Five-page Power BI dashboard — Executive, Customer, Fraud, Merchant, Governance

---

## Dashboard Pages

| Page | Purpose |
|---|---|
| Executive Overview | High level KPIs, revenue trends, fraud rate |
| Customer Intelligence | Segmentation, top customers, SCD audit trail |
| Fraud and Risk | Fraud patterns, high risk transactions, risk gauge |
| Merchant Analytics | Merchant performance, spend categories, weekend trends |
| Data Governance | Scorecard, dimensional change history, audit trail |

![Dashboard](screenshots/powerbi_dashboard.png)

---

## Dataset

Synthetic South African credit card transaction dataset:

- 300+ customers across 20 cities
- 60+ merchants across 5 categories
- 60 500+ transactions spanning 5 years
- Realistic fraud patterns — high risk merchants carry 15% fraud rate
- SCD Type 2 changes — customer relocations, card upgrades, 
  merchant category reclassifications

---

## Author

**Kulani Baloyi**  
Data Engineering | Microsoft Fabric | Power BI | Python | SQL
