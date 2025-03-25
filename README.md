# NYC Taxi Data Analytics: Multi-Platform Comparison (Databricks, BigQuery, Snowflake)

## Table of Contents
- [Overview](#overview)
  - [Performance and Cost Visualizations](#performance-and-cost-visualizations)
    - [Cost Comparison Charts](#cost-comparison-charts)
      - [Overall Cost Comparison (Excluding Copy Data Costs)](#overall-cost-comparison-excluding-copy-data-costs)
      - [Detailed Cost Breakdown by Provider](#detailed-cost-breakdown-by-provider)
      - [Transform Cost Comparison Across Platforms](#transform-cost-comparison-across-platforms)
    - [Performance Comparison Charts](#performance-comparison-charts)
      - [Overall Performance Comparison](#overall-performance-comparison)
      - [Detailed Performance Breakdown](#detailed-performance-breakdown)
- [Modifications from Original Workshop](#modifications-from-original-workshop)
  - [Replaced Transformation by Spark with Cloud SQL Data Warehouse](#-replaced-transformation-by-spark-with-cloud-sql-data-warehouse)
  - [Expanded Data Range for Better Benchmarking](#-expanded-data-range-for-better-benchmarking)
  - [Optimize Transformation Query for Big Dataset (Yellow Taxi)](#-optimize-transformation-query-for-big-dataset-yellow-taxi)
  - [Additional Enhancements](#additional-enhancements)
- [Architecture](#architecture)
  - [High-Level Architecture](#high-level-architecture)
  - [Data Ingestion Flow](#data-ingestion-flow)
    - [Batch Ingestion](#batch-ingestion)
      - [Azure](#azure)
      - [GCP](#gcp)
- [Setup](#setup)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Additional Setup Resources](#additional-setup-resources)
  - [Local Development to Databricks Synchronization](#local-development-to-databricks-synchronization)
- [Dataset Statistics](#dataset-statistics)
  - [Trip Data](#trip-data)
  - [Reference Data](#reference-data)
  - [Data Growth by Year](#data-growth-by-year)
  - [Storage Container Sizes](#storage-container-sizes)
- [Cost Analysis](#cost-analysis)
  - [Key Cost Considerations for Databricks Clusters](#key-cost-considerations-for-databricks-clusters)
- [Benchmark Results](#benchmark-results)
  - [SQL Query Optimization Results (Azure)](#-sql-query-optimization-results-azure)
  - [Data Pipeline Execution Times (Yellow Taxi)](#-data-pipeline-execution-times-yellow-taxi)
  - [Transform Method Evolution](#-transform-method-evolution)
- [Resources](#resources)
  - [Computing Resources](#-computing-resources)
  - [Storage](#-storage)
- [Project Structure](#project-structure)
- [Delta Lake](#delta-lake)
  - [Troubleshooting](#troubleshooting)
  - [Best Practices](#best-practices)
- [Future Enhancements](#future-enhancements)

## Overview

An experimental data engineering project for processing and analyzing NYC Taxi data (1.4B+ records) across multiple platforms (Databricks, BigQuery, Snowflake) and cloud vendors (Azure, GCP) to compare performance and cost. This project extends the [Azure-Databricks-NYC-Taxi-Workshop](https://github.com/microsoft/Azure-Databricks-NYC-Taxi-Workshop) with significant performance improvements by replacing Spark transformations with cloud-native SQL data warehousing solutions, expanding the data range (2009–2017), and optimizing queries using BROADCAST hints.

The project implements and benchmarks the same data pipeline across:
- **Azure Databricks**: Using Azure Data Lake Storage and Databricks SQL Warehouse
- **GCP Databricks**: Using Google Cloud Storage and Databricks SQL Warehouse
- **GCP BigQuery**: Using Google Cloud Storage and BigQuery for transformations
- **Snowflake**: Using Snowflake's data warehousing capabilities with data from GCP

This multi-platform approach demonstrates cloud-agnostic data engineering patterns while leveraging each platform's native services for storage, data warehousing, and secret management. The comprehensive cost and performance analysis helps data engineers make informed decisions when selecting platforms for large-scale data processing workloads.

### Performance and Cost Visualizations

This section provides visual representations of the performance and cost metrics for our NYC Taxi data processing pipeline across different cloud platforms.

#### Cost Comparison Charts

##### Overall Cost Comparison (Excluding Copy Data Costs)

![Cost Comparison Pie Chart](images/comparison/cost-comparison-pie.png)

This chart compares the total cost per run across different platforms, excluding copy data/egress costs. Azure offers the lowest cost at $19.16/run, while GCP costs vary depending on the transform option used: $38.31/run with BigQuery (left) or $33.17/run with Databricks SQL Warehouse (right).

##### Detailed Cost Breakdown by Provider

![Azure Cost Breakdown](images/comparison/azure-cost-breakdown.png)

Azure's costs are primarily driven by compute resources, particularly the Databricks cluster which accounts for the majority of the expenses.

![GCP Cost Breakdown](images/comparison/gcp-cost-breakdown.png)

GCP's cost structure is shown with two options: using BigQuery for transformations (left, $38.31/run) and using Databricks SQL Warehouse (right, $33.17/run). Both options show significant portions going to copy data and computing (CSV to parquet conversion), but differ in their transform costs.

##### Transform Cost Comparison Across Platforms

![Transform Cost Comparison](images/comparison/transform-cost-comparison.png)

This chart compares the transform costs across BigQuery, Databricks, and Snowflake. Databricks SQL Warehouse offers the most cost-effective solution at $2.70/run on GCP and $2.13/run on Azure, while Snowflake has the highest transform cost at $30.00/run.

#### Performance Comparison Charts

##### Overall Performance Comparison

![Performance Comparison Bar Chart](images/comparison/performance-comparison-bar.png)

GCP outperforms Azure in total execution time, completing the entire pipeline in approximately 94 minutes compared to Azure's 147 minutes.

##### Detailed Performance Breakdown

![Performance Breakdown Bar Chart](images/comparison/performance-breakdown-bar.png)

The detailed breakdown shows that while both platforms have similar patterns (with data conversion taking the most time), GCP significantly outperforms Azure in the transformation and materialization steps.

## Modifications from Original Workshop

This repository includes several tweaks and enhancements compared to the original Azure-Databricks-NYC-Taxi-Workshop:

### 🚀 Replaced Transformation by Spark with Cloud SQL Data Warehouse

Switched from Spark to Databricks Cloud SQL Data Warehouse for transformations due to significant performance improvements over the original workshop's slower Spark-based approach

### 📊 Expanded Data Range for Better Benchmarking

Extended data processing from the original 2016-2017 range to include all NYC taxi data from 2009-2017, creating a massive 1-billion-record dataset for more comprehensive analysis and performance testing

### 🔍 Optimize Transformation Query for Big Dataset (Yellow Taxi)

Added query optimization techniques (by using BROADCAST join hint and UNION ALL query approach) for better performance and ad-hoc queries on the large Yellow Taxi dataset (1.37B records)

## Architecture

### High-Level Architecture

![Overall Architecture](images/overall-architecture.png)

The architecture consists of several key components that are implemented differently across cloud providers:

#### Common Components

**Data Synchronization**  
Copies data from NYC source into cloud storage (stored as CSV)
- Azure: **Azure Data Factory**
- GCP: **Storage Transfer Service**

**Data Ingestion (Apache Spark)**  
Apache Spark on Databricks processes raw CSV data into optimized formats for analytics:
- Converts CSV to Parquet (reference data) and Delta Lake (trip data)
- Handles schema evolution and data partitioning
- Provides distributed processing for large-scale data transformation
- Enables efficient data processing with in-memory computation

**Secret Management**  
Stores sensitive information like secrets and credentials for connecting to Databricks and cloud services
- Azure: **Azure Key Vault**
- GCP: **Databricks secrets**

**Storage**  
Serves as the primary storage layer where we implement the medallion architecture (Bronze, Silver, Gold layers)
- Azure: **Azure Data Lake Storage Gen2**
- GCP: **Google Cloud Storage (GCS)**

**Cloud Data Warehouse**  
Provides the environment for data transformation and querying for reporting and analytics
- Azure: **Azure Synapse**
- GCP: **Google BigQuery**

### Data Ingestion Flow

#### Batch Ingestion

##### Azure

###### High Level Diagram
![Azure Batch Ingestion Flow](images/azure-batch-ingestion-flow.png)

###### Storage Layer Details

| Layer | Storage Type | Format | Purpose | Example Tables |
|-------|-------------|--------|---------|---------------|
| **Bronze** | Cloud Object Storage | CSV | Raw data storage | yellow_taxi_trips_raw, green_taxi_trips_raw |
| **Silver** | Cloud Object Storage | Parquet/Delta | Processed data | taxi_zone_lookup (Parquet), yellow_taxi_trips_transform (Delta) |
| **Gold** | Cloud Object Storage | Delta Lake | Analytics-ready data | taxi_trips_mat_view |

##### GCP

###### High Level Diagram
![GCP Batch Ingestion Flow](images/gcp-batch-ingestion-flow.png)

###### Storage Layer Details

| Layer | Storage Type | Format | Purpose | Example Tables |
|-------|-------------|--------|---------|---------------|
| **Bronze** | Cloud Object Storage | CSV | Raw data storage | yellow_taxi_trips_raw, green_taxi_trips_raw |
| **Silver** | BigQuery Table | N/A | Processed data | taxi_zone_lookup, yellow_taxi_trips_transform |
| **Gold** | BigQuery Table | N/A | Analytics-ready data | taxi_trips_mat_view |


## Setup

For a comprehensive setup guide, you can follow [module 01-Primer](https://github.com/microsoft/Azure-Databricks-NYC-Taxi-Workshop) in the original Azure-Databricks-NYC-Taxi-Workshop repository.

### Prerequisites

- Databricks workspace
- Azure Storage Account
- Python 3.11
- Databricks CLI configured
- VSCode with Databricks extensions installed:
  - [Databricks](https://marketplace.visualstudio.com/items?itemName=databricks.databricks) - Official Databricks extension for VSCode
  - [Databricks Notebooks](https://marketplace.visualstudio.com/items?itemName=paiqo.databricks-vscode) - For working with Databricks notebooks locally

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/Databricks-NYC-Taxi.git
   cd Databricks-NYC-Taxi
   ```

2. Install development dependencies:
   ```bash
   pip install -r requirements-dev.txt
   ```

3. Configure Databricks connection:
   ```bash
   databricks configure --token
   ```

4. Set up secrets for your cloud provider:

   #### For Azure:
   - Create an Azure Key Vault to store your secrets
   - In Databricks, create a secret scope named "azure-databricks" linked to your Azure Key Vault
   - Add the following secrets to your Azure Key Vault:
     - `warehouse-sql-host`: SQL warehouse host URL
     - `warehouse-sql-token`: SQL warehouse access token
     - `warehouse-sql-warehouseid`: SQL warehouse ID
     - `warehouse-sql-catalog-nyc`: SQL catalog name for NYC data
     - `warehouse-sql-schema-nyc`: SQL schema name for NYC data
     - `storage-name`: Azure Storage account name
     - `storage-key`: Azure Storage account key

   #### For GCP:
   - In Databricks, create a secret scope named "databricks-warehouse"
   - Add the following secrets directly in Databricks:
     - `sql-host`: SQL warehouse host URL
     - `sql-token`: SQL warehouse access token
     - `warehouseid`: SQL warehouse ID

### Additional Setup Resources

- For setting up Databricks and VSCode integration:
  - [The Ultimate Setup for Databricks Development with VSCode](https://medium.com/@joelpantoja/the-ultimate-setup-for-databricks-development-with-vscode-490bae1b5a7a)

- For setting up Azure cloud using UI:
  - [Azure End-to-End Data Engineering Project - Part 1](https://rihab-feki.medium.com/azure-end-to-end-data-engineering-project-incremental-data-pipeline-part-1-ed3e55767513)
  - [Azure End-to-End Data Engineering Project - Part 2](https://rihab-feki.medium.com/azure-end-to-end-data-engineering-project-medallion-architecture-with-databricks-part-2-9abf1ab3dba0)

### Local Development to Databricks Synchronization

This project includes utilities to synchronize notebooks and SQL files from your local environment to Databricks workspace:

- Sync Jupyter notebooks to Databricks:
  ```bash
  ./sync_notebook.sh Workspace/CarsProject/jupyter-notebook/LoadDataYellowTaxi.ipynb /Users/me/LoadDataYellowTaxi
  ```

- Sync SQL transformation files to Databricks:
  ```bash
  ./sync_sql.sh Workspace/CarsProject/sql/transform /Users/me/sql/transform
  ```

## Dataset Statistics

The project processes a massive volume of NYC Taxi data:

### Trip Data

| Dataset | Time Period | Records | Raw CSV Size | Compressed Size (Delta) | Compression Ratio | Partitioning |
|:-------:|:-----------:|:-------:|:------------:|:-----------------------:|:-----------------:|:------------:|
| **Yellow Taxi Trips** | 2009-2017 | **1.37B** | 223.13GB (total) | 82.04GB | **2.6:1** | Year, Month |
| **Green Taxi Trips** | 2013-2017 | **59M** | (included above) | 3.73GB | **2.6:1** | Year, Month |

### Reference Data

| Dataset | Records | Format | Description |
|:-------:|:-------:|:------:|:------------|
| **Taxi Zone Lookup** | 265 | Parquet | Geographic zones for pickup/dropoff locations |
| **Vendor Lookup** | 3 | Parquet | Taxi service providers |
| **Payment Type Lookup** | 6 | Parquet | Methods of payment (cash, credit card, etc.) |
| **Rate Code Lookup** | 6 | Parquet | Different rate categories |
| **Trip Type Lookup** | 3 | Parquet | Types of trips (street-hail, dispatch, etc.) |

### Data Growth by Year

| Year | Yellow Taxi Records | Green Taxi Records | Total Records |
|:----:|:-------------------:|:------------------:|:-------------:|
| 2009 | ~170M | - | ~170M |
| 2010 | ~168M | - | ~168M |
| 2011 | ~176M | - | ~176M |
| 2012 | ~179M | - | ~179M |
| 2013 | ~173M | ~6M | ~179M |
| 2014 | ~165M | ~13M | ~178M |
| 2015 | ~146M | ~16M | ~162M |
| 2016 | ~131M | ~14M | ~145M |
| 2017 | ~62M | ~10M | ~72M |
| **Total** | **~1.37B** | **~59M** | **~1.43B** |

### Storage Container Sizes

| Container | Size |
|:---------:|:----:|
| Bronze | 223.13GB |
| Silver | 107.2GB |
| Gold | 124.04GB |

## Cost Analysis

We tracked the costs associated with running our data pipeline across both cloud platforms:

| Cloud Provider | Resource Type | Operation | Service | Cost (USD) |
|:--------------:|:-------------:|:---------:|:-------:|:----------:|
| **Azure** | **Storage** | Daily Storage | Azure Data Lake | $1.39/day |
| | **Copy Data** | Data Transfer | Azure Data Factory | $2.66/run |
| | **Compute** | Convert CSV to Parquet | VM Instance | $1.88/run |
| | | | Databricks Cluster Spark Computing | $11.10/run |
| | | Transform Data | Databricks SQL Warehouse | $2.13/run |
| | **TOTAL** | | | **$19.16/run** + $1.39/day |
|||||
| **GCP** | **Storage** | Daily Storage | GCS + BigQuery | $0.67/day |
| | **Copy Data** | Data Egress | Storage Transfer | $12.98/run |
| | **Compute** | Convert CSV to Parquet | VM Instance | $2.60/run |
| | | | Databricks Cluster Spark Computing | $14.22/run |
| | | Transform Data (Option 1) | BigQuery | $7.84/run |
| | | Transform Data (Option 2) | Databricks SQL Warehouse | $2.70/run |
| | **TOTAL (with BigQuery)** | | | **$38.31/run** + $0.67/day |
| | **TOTAL (with Databricks SQL)** | | | **$33.17/run** + $0.67/day |
|||||
| **Snowflake** | **Copy Data** | Egress Copy from GCP | Snowflake | $6.37/run |
| | **Compute** | Transform Data | Snowflake | $30.00/run |
| | **TOTAL** | | | **$36.37/run** |

### Key Cost Insights

- **Total Cost Comparison**: 
  - **Azure** offers the lowest total cost at **$19.16/run** (plus daily storage)
  - **GCP with Databricks SQL** costs **$33.17/run** (plus daily storage)
  - **GCP with BigQuery** costs **$38.31/run** (plus daily storage)
  - **Snowflake** costs **$36.37/run**

- **Storage Cost Comparison**: Azure storage costs ($1.39/day) are approximately twice as expensive as GCP storage ($0.67/day) for similar workloads and data volumes.
  
- **Copy Data Considerations**: When ingesting data, be mindful of data copy/egress charges when moving data between different cloud vendors. These charges can be significant ($12.98/run in our GCP implementation) and should be avoided when possible by keeping data processing within a single cloud ecosystem.

- **Transform Cost Comparison**: There are significant differences in transform costs across platforms:
  - Databricks SQL Warehouse is the most cost-effective option on both Azure ($2.13/run) and GCP ($2.70/run)
  - BigQuery is moderately priced at $7.84/run
  - Snowflake is the most expensive at $30.00/run, plus additional egress costs ($6.37/run) when copying data from GCP

### Key Observations

1. **Cost vs. Performance Tradeoff**: 
   - **Azure** offers the **lowest overall cost** ($19.16/run) but slower performance
   - **GCP** provides **fastest processing** but at a **higher cost** ($33.17-$38.31/run)
   - **Snowflake** has the **highest transform cost** ($30.00/run) with moderate performance

2. **Transformation Speed**: 
   - GCP's BigQuery significantly outperforms Azure's Databricks SQL Warehouse for transformation tasks (1.62 min vs. 11.47 min)
   - GCP's materialization is also much faster (1.05 min vs. 14.43 min)
   - Snowflake's performance falls between Azure and GCP (26.41 min for transform, 23.54 min for materialize)

3. **Cost Structure Differences**:
   - Azure's costs are dominated by compute (Databricks cluster)
   - GCP's costs are heavily influenced by copy data charges
   - Snowflake has high transform costs plus additional copy data charges when copying data from other cloud providers

4. **Platform Selection Considerations**:
   - For cost-sensitive workloads: Databricks SQL Warehouse on Azure offers the best value
   - For performance-critical workloads: BigQuery on GCP provides the fastest processing
   - For cross-cloud scenarios: Consider the additional copy data costs when moving data between platforms

## Benchmark Results

#### Key Findings

- **Best Overall Performance**: BROADCAST hint combined with union query approach (13min 14s for full dataset)
- **Full Dataset Queries**: Combining BROADCAST and union query improves performance by 40% compared to original approach
- **Ad-hoc Queries (LIMIT 1000)**: BROADCAST hint delivers dramatic speedup (from minutes to seconds)
- **Recommendation**: Use BROADCAST hints for all queries, especially for interactive/ad-hoc analysis

### 🕒 Data Pipeline Execution Times (Yellow Taxi)

The following execution times were measured for processing the Yellow Taxi dataset (the largest dataset with 1.37B records) through the medallion architecture:

| Platform | Cloud Provider | Processing Step | Data Stage Transition | Execution Time | Details |
|:--------:|:--------------:|:---------------:|:---------------------:|:--------------:|:--------|
| Databricks | **Azure** | **Convert CSV to Parquet** | Bronze → Silver | 120min 39s | Initial data ingestion and conversion |
| | | **Transform** | Silver → Silver (transformed) | 11min 28s | Create table from transform SQL run in Databricks SQL Warehouse |
| | | **Materialize** | Silver → Gold | 14min 26s | Final materialization step in Databricks SQL Warehouse |
| Databricks | **GCP** | **Convert CSV to Parquet** | Bronze → Silver | 91min | Initial data ingestion and conversion |
| | | **Transform** | Silver → Silver (transformed) | 1min 37s | Create table from transform SQL run in Cloud SQL Warehouse (BigQuery) |
| | | **Materialize** | Silver → Gold | 1min 3s | Final materialization step in Cloud SQL Warehouse (BigQuery) |
| Snowflake | **GCP** | **Transform** | Silver → Silver (transformed) | 26min 41s | Create table from transform SQL run in Snowflake |
| | | **Materialize** | Silver → Gold | 23min 54s | Final materialization step in Snowflake |

### 🔄 Transform Method Evolution

We experimented with different transformation approaches before finding the optimal solution across both cloud platforms:

| Cloud Provider | Transform Method | Execution Time | Details |
|:--------------:|:----------------:|:--------------:|:--------|
| **Azure** | **Raw Spark (Original Workshop)** | Too long to complete | Initial approach using raw Spark to process parquet files directly |
| | **Hybrid Spark** ([AzureTransformDataYellowTaxiSpark.ipynb](Workspace/CarsProject/jupyter-notebook/azure/transform-data/AzureTransformDataYellowTaxiSpark.ipynb)) | >4 hours | Second approach with two phases: |
| | | ~2.1 hours | Parallel JDBC read using pickup_datetime partitioning to prevent data skew |
| | | ~2.3 hours | Spark-based transformation and storage write |
| | **Databricks SQL Datawarehouse** | **11min 28s** | Final approach running SQL transformations directly in Databricks SQL |
| | **Cloud SQL Warehouse (Azure Synapse)** | **`N/A`** | Unable to run due to compute capacity exceed |
|||||
| **GCP** | **Raw Spark (Original Workshop)** | Too long to complete | Initial approach using raw Spark to process parquet files directly |
| | **Hybrid Spark** ([GCPTransformDataYellowTaxiSpark.ipynb](Workspace/CarsProject/jupyter-notebook/gcp/transform-data/GCPTransformDataYellowTaxiSpark.ipynb)) | 3.5 hours | Second approach with two phases: |
| | | 2.4 hours | Parallel JDBC read using pickup_datetime partitioning to prevent data skew |
| | | 1.1 hours | Spark-based transformation and storage write |
| | **Databricks SQL Datawarehouse** | **14min 55s** | Intermediate approach using Databricks SQL |
| | **Cloud SQL Warehouse (BigQuery)** | **1min 20s** | Final approach running SQL transformations directly in BigQuery |

> **Note:** 
> 
> The dramatic performance improvements from Spark-based approaches (3.5+ hours) to SQL-based transformations demonstrate why we switched to cloud-native data warehousing solutions for these workloads.
> 
> In Azure, we can't run transform queries in Azure Synapse data warehouse due to compute capacity exceed. The best execution time (11min 28s) is achieved by running SQL transformations directly in Databricks SQL Data Warehouse.
> 
> In GCP, we can run transforms in BigQuery for optimized performance, achieving an impressive 1min 20s execution time.

## Resources

> **Note:** All resources are provisioned in the asia-southeast1 region (Singapore) in both GCP and Azure.

### 🚀 SQL Query Optimization Results (Azure)

We conducted performance testing on complex join operations between taxi trip data and reference tables using different optimization techniques on Azure Databricks. The benchmark query ([1-join-yellow-taxi.sql](Workspace/CarsProject/sql/benchmark/1-join-yellow-taxi.sql)) analyzes trip patterns and payment distributions across different NYC taxi zones.

#### Optimization Techniques Tested

1. **Original vs. Union Query**: 
   - Original: Standard join approach
   - Union: Alternative implementation using UNION ALL to combine results

2. **BROADCAST Join Hint**:
   - Explicitly tells the query optimizer to broadcast smaller tables to all nodes

#### Performance Results

**Full Dataset Query (Complete Results)**

| Approach | Without BROADCAST | With BROADCAST | Improvement |
|:--------:|:-----------------:|:--------------:|:-----------:|
| Original Query | 23min 57s | 21min 27s | 10% faster |
| Union Query | 21min 16s | **13min 14s** | **45% faster** |

**Limited Dataset Query (LIMIT 1000)**

| Approach | Without BROADCAST | With BROADCAST | Improvement |
|:--------:|:-----------------:|:--------------:|:-----------:|
| Original Query | 8min 47s | 20.117s | 96% faster |
| Union Query | 9min 22s | **19.586s** | 96% faster |

### 💻 Computing Resources

The following computing environment was used for all benchmarks and data processing:

#### Databricks

##### Compute Cluster

| Cloud Provider | Resource Type | Specification | Details |
|:--------------:|:-------------:|:-------------:|:--------|
| **Azure** | **Cluster Type** | Personal Compute | Single node, single worker |
| | **Runtime Version** | 16.2 | Apache Spark 3.5.2, Scala 2.12 |
| | **Node Type** | Standard_DS4_v2 | 28 GB Memory, 8 Cores |
| **GCP** | **Cluster Type** | Personal Compute | Single node, single worker |
| | **Runtime Version** | 16.2 | Apache Spark 3.5.2, Scala 2.12 |
| | **Node Type** | n2-highmem-8 | 64 GB Memory, 8 Cores |

##### SQL Warehouse

| Resource Type | Specification | Details |
|:-------------:|:-------------:|:--------|
| **Warehouse Name** | Serverless Starter Warehouse | Serverless type |
| **Cluster Size** | Small | 12 DBU/h/cluster |
| **Auto Stop** | Enabled | After 10 minutes of inactivity |
| **Scaling** | 1-1 clusters | 12 to 12 DBU capacity range |

##### Provision notes for cost optimizing

When provisioning Databricks computing clusters, consider these cost-optimization factors:

- **Spot Instances**: Evaluate whether to use spot instances for significant cost savings on non-critical workloads.
  
- **Photon Engine**: Enable the Photon engine to optimize Spark SQL performance. This significantly impacts DBU pricing - for example, an n2-highmem-4 instance costs 1.96 DBU/hour with Photon versus 0.96 DBU/hour without Photon.
  
- **Compute Type Selection**: Choose the appropriate compute type based on your workload requirements:
  - All-purpose compute: For interactive development
  - Job compute: For scheduled production workloads
  - SQL compute: For data warehousing operations
  - SQL serverless: For on-demand query processing
  
Each option offers different pricing models and performance characteristics.

#### Snowflake

##### Compute Warehouse

| Specification | Details |
|---------------|---------|
| **Size** | Medium |
| **Type** | Snowpark-optimized |
| **Resource Constraint** | MEMORY_16X |
| **Compute Resources** | 32 GB Memory, 8 cores per node, 4 nodes (total: 256 GB Memory, 32 cores) |

### 🗄️ Storage

| Cloud Provider | Storage Type | Configuration | Details |
|:--------------:|:------------:|:-------------:|:--------|
| **Azure** | **Azure Data Lake Storage Gen2** | Standard tier | Hierarchical namespace enabled |
| **GCP** | **Google Cloud Storage** | Standard storage class | Non-hierarchical namespace (flatten) |

## Project Structure

The project follows a modular structure to separate different stages of the data pipeline, with cloud-specific implementations:

```
.
├── README.md
├── requirements-dev.txt
├── sync_sql.sh
├── images/
│   ├── overall-architecture.png
│   └── batch-ingestion-flow.png
│
└── Workspace/
    ├── databricks_to_jupyter.py
    ├── jupyter_to_databricks.py
    ├── 01-General/
    │   └── 2-CommonFunctions.ipynb
    │
    └── CarsProject/
        ├── jupyter-notebook/
        │   ├── azure/
        │   │   ├── analytics/
        │   │   │   └── Report.ipynb
        │   │   ├── load-data/
        │   │   │   ├── AzureLoadDataGreenTaxi.ipynb
        │   │   │   ├── AzureLoadDataYellowTaxi.ipynb
        │   │   │   └── AzureLoadReferenceData.ipynb
        │   │   └── transform-data/
        │   │       ├── AzureTransformData.ipynb
        │   │       └── AzureTransformDataYellowTaxiSpark.ipynb
        │   │
        │   └── gcp/
        │       ├── analytics/
        │       │   └── Report.ipynb
        │       ├── create-secret.sh
        │       ├── load-data/
        │       │   ├── GCPLoadDataGreenTaxi.ipynb
        │       │   ├── GCPLoadDataYellowTaxi.ipynb
        │       │   └── GCPLoadReferenceData.ipynb
        │       ├── transform-data/
        │       │   ├── GCPTransformData.ipynb
        │       │   ├── GCPTransformDataBigquery.ipynb
        │       │   └── GCPTransformDataYellowTaxiSpark.ipynb
        │
        └── sql/
            ├── benchmark/
            │   └── 1-join-yellow-taxi.sql
            └── transform/
                ├── azure/
                │   └── databricks/
                │       ├── 1-transform-yellow-taxi.sql
                │       ├── 2-transform-green-taxi.sql
                │       └── 3-transform-create-materialize-view.sql
                └── gcp/
                    ├── bigquery/
                    │   ├── 1-bq-transform-yellow-taxi.sql
                    │   ├── 2-bq-transform-green-taxi.sql
                    │   ├── 3-bq-transform-create-materialize-view.sql
                    │   └── gcp_billing_by_label.sql
                    └── databricks/
                        ├── 1-transform-yellow-taxi.sql
                        ├── 2-transform-green-taxi.sql
                        └── 3-transform-create-materialize-view.sql
```

Each component in this structure serves a specific purpose in the data pipeline:

- **Notebooks**: Organized by cloud provider (Azure/GCP) and pipeline stage (load/transform/analytics)
- **SQL Scripts**: Separated by cloud provider and execution environment (Databricks/BigQuery)
- **Utility Scripts**: For conversion between notebook formats and synchronization with Databricks
- **Configuration Files**: For project settings and environment setup

## Delta Lake

### Troubleshooting

When working with Delta Lake tables across different environments (Databricks and BigQuery), you may encounter these common issues:

**Metadata Schema Errors in BigQuery**
