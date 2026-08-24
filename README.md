# Spotify Azure Data Engineering Pipeline

An end-to-end data engineering project built on Microsoft Azure, covering data ingestion, incremental loading, cloud storage, transformation, dimensional modelling, and Databricks deployment.

The project simulates a Spotify data platform where data is ingested from **Azure SQL Database**, orchestrated using **Azure Data Factory**, stored in **Azure Data Lake Storage Gen2**, and transformed using **Azure Databricks and PySpark** following the Bronze–Silver–Gold Medallion Architecture.

---

## Architecture

```text
Azure SQL Database
        │
        ▼
Azure Data Factory
        │
        │  Incremental ingestion
        │  CDC / Watermark
        ▼
Azure Data Lake Storage Gen2
        │
        ▼
     Bronze Layer
     Raw Parquet Data
        │
        ▼
 Azure Databricks
   PySpark / Spark
        │
        ▼
     Silver Layer
  Cleaned & Transformed
        │
        ▼
 Databricks Lakeflow
 Declarative Pipelines
        │
        ▼
      Gold Layer
        │
   ┌────┼──────────────┐
   │    │              │
DimUser DimTrack    DimDate
   │    │              │
   └────┴── FactStream ┘
        │
        ▼
 Analytics-Ready Data
```

---

## Project Overview

The goal of this project was to build a complete Azure data pipeline rather than working with each service independently.

The pipeline starts with data stored in Azure SQL Database. Azure Data Factory handles ingestion and orchestration, while ADLS Gen2 acts as the data lake. Databricks is then used to process and transform the data into structured Silver and Gold layers.

Instead of performing full loads every time the pipeline runs, the ingestion process uses a CDC/watermark approach to identify and load only new or updated records.

The final Gold layer organizes the data into fact and dimension tables that can be consumed by downstream analytical workloads.

---

## Data Flow

### 1. Source — Azure SQL Database

Azure SQL Database acts as the source system.

The ingestion process keeps track of the previously processed CDC value and uses it to determine which records need to be extracted during the next pipeline run.

This allows the pipeline to perform incremental loads instead of repeatedly reading the complete source tables.

---

### 2. Incremental Ingestion — Azure Data Factory

Azure Data Factory is responsible for moving data from Azure SQL Database into ADLS Gen2.

The ingestion pipeline uses a dynamic query based on the configured CDC column and the previously processed value.

```sql
SELECT *
FROM source_table
WHERE cdc_column > last_processed_cdc
```

The ADF implementation includes:

* Parameterized pipelines and datasets
* Lookup activities
* CDC/watermark-based incremental ingestion
* Dynamic SQL queries
* ForEach orchestration
* Conditional processing

The pipeline also checks whether new records were returned before continuing with downstream processing.

---

### 3. Metadata-Driven Processing

Rather than creating a separate ingestion pipeline for every source table, the ingestion logic is reusable.

Information such as the schema, table name and CDC column is passed to the pipeline dynamically.

A ForEach activity iterates through the configured source tables and invokes the same incremental ingestion process for each one.

This keeps the pipeline easier to maintain and allows additional source tables to be incorporated without duplicating the complete ingestion logic.

---

### 4. Bronze Layer — ADLS Gen2

Data extracted from Azure SQL is written to the Bronze layer in Azure Data Lake Storage Gen2.

The Bronze layer acts as the raw landing area and preserves the source data before transformation.

Data is stored in **Parquet format**, providing an efficient columnar format for downstream Spark processing.

```text
Azure SQL
    ↓
Azure Data Factory
    ↓
ADLS Gen2 / Bronze
    ↓
Raw Parquet Data
```

---

### 5. Silver Layer — Databricks & PySpark

Azure Databricks processes the Bronze data using PySpark.

The Silver layer is used to clean, standardize and prepare the source data before analytical modelling.

Keeping the raw and transformed layers separate ensures that the original ingested data remains available while downstream transformations can evolve independently.

```text
Bronze
   ↓
PySpark Transformations
   ↓
Silver
```

---

### 6. Gold Layer — Analytical Model

The final processing stage creates analytics-ready tables from the Silver layer.

The Gold layer is organized using fact and dimension modelling.

The current model contains:

**Dimensions**

* `DimUser`
* `DimTrack`
* `DimDate`

**Fact**

* `FactStream`

Conceptually:

```text
              DimUser
                 │
                 │
DimDate ──── FactStream ──── DimTrack
```

This separates descriptive attributes from streaming events and produces a structure suitable for analytical queries and downstream BI workloads.

---

## Databricks Lakeflow Declarative Pipelines

Gold-layer transformations are organized using **Databricks Lakeflow Declarative Pipelines (DLT)**.

Individual transformation modules are maintained for the fact and dimension tables:

```text
gold/
└── dlt/
    └── transformations/
        ├── DimUser.py
        ├── DimTrack.py
        ├── DimDate.py
        └── FactStream.py
```

Separating transformation logic into individual modules makes the processing layer easier to understand, maintain and extend.

---

## Unity Catalog

The Databricks implementation uses Unity Catalog concepts to organize data assets through catalogs and schemas.

Catalog and schema values are kept configurable instead of being hardcoded throughout the transformation code.

This configuration is also integrated with the Databricks Asset Bundle setup.

---

## Databricks Asset Bundles

The Databricks portion of the project is managed using **Databricks Asset Bundles**.

The bundle contains the source code, pipeline resource definitions and environment-specific configuration required to deploy the Databricks workload.

Separate targets are configured for:

```text
Development
Production
```

This provides a repeatable way to validate and deploy Databricks resources instead of relying entirely on manual workspace configuration.

---

## Technology Stack

| Area                   | Technology                                |
| ---------------------- | ----------------------------------------- |
| Cloud                  | Microsoft Azure                           |
| Source                 | Azure SQL Database                        |
| Orchestration          | Azure Data Factory                        |
| Data Lake              | Azure Data Lake Storage Gen2              |
| Processing             | Azure Databricks                          |
| Distributed Processing | Apache Spark                              |
| Programming            | Python, PySpark                           |
| Data Pipeline          | Databricks Lakeflow / DLT                 |
| Governance             | Unity Catalog                             |
| Storage Format         | Parquet                                   |
| Architecture           | Bronze–Silver–Gold Medallion Architecture |
| Data Modelling         | Fact & Dimension Modelling                |
| Deployment             | Databricks Asset Bundles                  |
| Version Control        | Git, GitHub                               |

---

## Repository Structure

```text
azure_de_project/
│
├── dataset/                       # ADF dataset definitions
├── factory/                       # Azure Data Factory configuration
├── linkedService/                 # Source and storage connections
├── pipeline/                      # ADF ingestion pipelines
│
├── spotify_dab/                   # Databricks project
│   ├── databricks.yml             # Asset Bundle configuration
│   ├── resources/                 # Databricks resource definitions
│   │
│   ├── src/
│   │   ├── silver/                # Silver-layer processing
│   │   │
│   │   └── gold/
│   │       └── dlt/
│   │           └── transformations/
│   │               ├── DimUser.py
│   │               ├── DimTrack.py
│   │               ├── DimDate.py
│   │               └── FactStream.py
│   │
│   ├── utils/                     # Shared transformation utilities
│   ├── requirements.txt
│   └── pyproject.toml
│
├── publish_config.json
└── README.md
```

---

## What This Project Covers

Through this project, I worked with the complete flow of data across an Azure-based data engineering platform:

**Ingestion & Orchestration**

* Azure Data Factory
* Incremental loading
* CDC/watermark logic
* Parameterized pipelines
* Metadata-driven ingestion
* ForEach orchestration

**Storage & Architecture**

* Azure Data Lake Storage Gen2
* Parquet
* Bronze–Silver–Gold Medallion Architecture

**Transformation & Modelling**

* Azure Databricks
* Apache Spark
* Python and PySpark
* Fact and dimension modelling
* Databricks Lakeflow Declarative Pipelines

**Governance & Deployment**

* Unity Catalog
* Databricks Asset Bundles
* Development and production configurations
* Git and GitHub

---

## Author

**Dhivagar G**

[GitHub Profile](https://github.com/DhivakarGovind)
