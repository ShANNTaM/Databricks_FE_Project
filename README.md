# E-Commerce Data Engineering Project — Databricks Medallion Architecture

An end-to-end batch data engineering pipeline built on **Databricks** that ingests raw e-commerce CSV data, cleanses and conforms it through a **medallion architecture** (Bronze → Silver → Gold), and produces BI-ready star-schema tables in **Unity Catalog**.

## Table of Contents

* [Architecture Overview](#architecture-overview)
* [Project Structure](#project-structure)
* [Data Sources](#data-sources)
* [Pipeline Layers](#pipeline-layers)
* [Gold Layer Data Model](#gold-layer-data-model)
* [How to Run](#how-to-run)
* [Tech Stack](#tech-stack)

## Architecture Overview

```
Raw CSVs (UC Volume)
/Volumes/ecommerce/source_data/raw/
        │
        ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐
│   BRONZE    │───▶│   SILVER    │───▶│        GOLD          │
│ as-ingested │    │ cleansed &  │    │ star-schema,         │
│ + audit    │    │ conformed   │    │ BI-ready facts/dims  │
│ columns    │    │ + typed     │    │ + derived measures   │
└─────────────┘    └─────────────┘    └─────────────────────┘
  ecommerce.bronze    ecommerce.silver     ecommerce.gold
```

* **Bronze** — raw data landed as-is with explicit schemas, plus audit columns (`ingested_at` / `ingest_timestamp`, `_source_file` / `file_name` captured from file metadata).
* **Silver** — deduplicated, trimmed, standardized codes, correct data types.
* **Gold** — conformed dimension and fact tables with derived measures, region enrichment, and a common currency (INR).

## Project Structure

```
Databricks_FE_Project_Ecommerce/
├── README.md
├── 01_setup/
│   └── catalog_and_schema_setup      # Create catalog, schemas, and volumes
├── 02_medallion_processing_dim/
│   ├── dim_bronze                     # Ingest dimension sources → bronze
│   ├── dim_silver                     # Cleanse & conform → silver
│   └── dim_gold                       # Build gold dimension tables
├── 03_Medallion_Processing_Fact/
│   ├── 1_fact_bronze                  # Ingest order_items → bronze
│   ├── 2_fact_silver                  # Cleanse & type-cast → silver
│   └── 3_fact_gold                    # Build BI-ready fact table → gold
└── ecomm-raw-data/                    # Original source CSVs
    ├── brands/brands.csv
    ├── category/category.csv
    ├── customers/customers.csv
    ├── date/date.csv
    ├── products/products.csv
    └── order_items/landing/*.csv
```

## Data Sources

Six e-commerce CSV sources (staged in the `ecomm-raw-data/` folder and loaded into the UC volume `/Volumes/ecommerce/source_data/raw/`):

| Source | Grain | Key Columns |
| --- | --- | --- |
| `brands.csv` | One row per brand | brand_code, brand_name, category_code |
| `category.csv` | One row per category | category_code, category_name |
| `customers.csv` | One row per customer | customer_id, phone, country_code, country, state |
| `date.csv` | One row per date | date, year, day_name, quarter, week_of_year |
| `products.csv` | One row per product | product_id, sku, category_code, brand_code, color, size, material, weight_grams, rating_count, … |
| `order_items/*.csv` | One row per order line item | dt, order_ts, customer_id, order_id, item_seq, product_id, quantity, unit_price_currency, unit_price, discount_pct, tax_amount, channel, coupon_code |

> Note: numeric source columns (e.g., `weight_grams`, `length_cm`) are deliberately read as strings in bronze because the incoming data contains anomalies; they are handled downstream.

## Pipeline Layers

### 1. Setup — `01_setup/catalog_and_schema_setup`

Creates the Unity Catalog objects the pipeline depends on:

* Catalog: `ecommerce`
* Schemas: `ecommerce.bronze`, `ecommerce.silver`, `ecommerce.gold`
* Source volume: `/Volumes/ecommerce/source_data/raw/` (holding the staged CSVs)

### 2. Dimension Processing — `02_medallion_processing_dim/`

**Bronze (`dim_bronze`)** — reads each CSV with an explicit `StructType` schema, adds audit columns, and writes:

* `brz_brand`, `brz_category`, `brz_customer`, `brz_date`, `brz_products` in `ecommerce.bronze`

**Silver (`dim_silver`)** — cleanses and standardizes each bronze table, e.g.:

* Trims whitespace in `brand_name` / `brand_code`; strips non-alphanumeric characters from `brand_code`
* Standardizes inconsistent category codes (`BOOKS→BKS`, `GROCERY→GRCY`, `TOYS→TOY`)
* Removes duplicates (e.g., `dropDuplicates()` on category/customer)
* Writes `slv_brands`, `slv_category`, `slv_customer`, `slv_date`, `slv_products` in `ecommerce.silver`

**Gold (`dim_gold`)** — builds conformed dimensions:

* `gld_dim_products` — products LEFT JOINed to a brands×category lookup; unmatched names defaulted to `'Not Available'`
* `gld_dim_customers` — customers enriched with a hand-mapped `region` per (country, state) for India, Australia, UK, US, UAE, Singapore, and Canada; unmapped regions defaulted to `'Other'`
* `gld_dim_date` — date dimension with `date_id` (`ddMMyyyy`), `month_name`, and `is_weekend` flag

### 3. Fact Processing — `03_Medallion_Processing_Fact/`

**Bronze (`1_fact_bronze`)** — ingests multi-file `order_items/landing/*.csv` with an explicit schema plus `file_name` and `ingest_timestamp` audit columns into `brz_order_items`.

**Silver (`2_fact_silver`)** — cleanses and type-casts the fact data:

* Deduplicates on (`order_id`, `item_seq`)
* Maps textual quantities (`"Two"` → `2`) and casts to int
* Strips `$` from `unit_price`, `%` from `discount_pct`, non-numerics from `tax_amount`; casts to numeric types
* Normalizes `coupon_code` (lower/trim) and `channel` (`web→Website`, `app→Mobile`)
* Parses `dt` → date and `order_ts` → timestamp (with a fallback pattern for mixed formats)
* Adds `processed_time`; writes `slv_order_items`

**Gold (`3_fact_gold`)** — builds the BI-ready fact table `gld_fact_order_items`:

* Derived measures: `gross_amount`, `discount_amount`, `net_amount` (gross − discount + tax)
* `date_id` key (`yyyyMMdd`) linking to `gld_dim_date`
* `coupon_flag` (1 if a coupon was applied)
* Currency conversion to a common **INR** reporting currency using fixed FX rates, producing `net_amount_inr`

## Gold Layer Data Model

Star schema in the `ecommerce.gold` schema:

```
                    ┌────────────────────┐
                    │  gld_dim_date      │
                    └─────────┬──────────┘
                              │ date_id
┌──────────────────┐   ┌──────▼───────────────┐   ┌──────────────────────┐
│ gld_dim_customers│──▶│ gld_fact_order_items │◀──│  gld_dim_products    │
└──────────────────┘   └──────────────────────┘   └──────────────────────┘
     customer_id        (order / line-item grain)         product_id
```

## How to Run

Run the notebooks in order (each is a standalone PySpark/SQL notebook — **Run All** works):

1. `01_setup/catalog_and_schema_setup` — create the catalog, schemas, and volume
2. `02_medallion_processing_dim/dim_bronze` → `dim_silver` → `dim_gold`
3. `03_Medallion_Processing_Fact/1_fact_bronze` → `2_fact_silver` → `3_fact_gold`

> Prerequisite: the source CSVs must be present under `/Volumes/ecommerce/source_data/raw/` (the copies in `ecomm-raw-data/` are the originals).

## Tech Stack

* **Databricks** notebooks (PySpark + Spark SQL) on serverless compute
* **Delta Lake** — all tables written as Delta with `overwrite` mode
* **Unity Catalog** — governed `ecommerce` catalog with bronze/silver/gold schemas and a UC Volume for raw files
* **Medallion Architecture** — bronze (raw + audit), silver (clean + typed), gold (star schema)
