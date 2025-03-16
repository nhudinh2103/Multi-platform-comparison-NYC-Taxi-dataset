# NYC Taxi Data Analytics with Databricks

A comprehensive data engineering solution for processing, analyzing, and visualizing New York City taxi trip data using Databricks, Delta Lake, and Azure Data Services. This project is inspired by [Azure-Databricks-NYC-Taxi-Workshop](https://github.com/microsoft/Azure-Databricks-NYC-Taxi-Workshop).

This project provides an end-to-end solution for processing and analyzing NYC Taxi trip data (Yellow and Green taxis). It demonstrates a modern data engineering approach using Databricks for data processing, Delta Lake for reliable data storage, and Azure Data Services for cloud infrastructure.

Key features:
- Data ingestion from CSV source with varying schemas
- Schema homogenization and data transformation
- Delta Lake format with partitioning
- Comprehensive data analysis and reporting capabilities

## Modifications from Original Workshop

This repository includes several tweaks and enhancements compared to the original Azure-Databricks-NYC-Taxi-Workshop:

### 🚀 Replaced Spark with SQL Cloud Datawarehouse

Switched from Spark to Databricks SQL Cloud Datawarehouse for transformations due to significant performance improvements over the original workshop's slower Spark-based approach

### 📊 Expanded Data Range for Better Benchmarking

Extended data processing from the original 2016-2017 range to include all NYC taxi data from 2009-2017, creating a massive 1-billion-record dataset for more comprehensive analysis and performance testing

### Additional Enhancements

- Added utilities for converting between Databricks and Jupyter notebook formats
- Implemented synchronization scripts for notebooks and SQL files
- Enhanced schema handling for different taxi data versions
- Added support for local development with VSCode

## Architecture

### High-Level Architecture

![Overall Architecture](images/overall-architecture.png)

The architecture consists of several key components:

**Azure Data Factory**  
Synchronizes data from NYC source to our container storage (stored as CSV)

**Apache Spark**  
Used for data ingestion to read and convert CSV to columnar compressed files:
- Fixed schema CSV data tables (reference data) are stored in Parquet format
- Dynamic schema with schema evolution (taxi trip data) is stored in Delta Lake format

**Azure Key Vault**  
Stores sensitive information like secrets and credentials for connecting to Databricks and Azure services

**Azure Data Lake Storage Gen2**  
Serves as the primary storage layer where we implement the medallion architecture (Bronze, Silver, Gold layers)

**Databricks SQL Data Warehouse**  
Provides the environment for data transformation and querying for reporting and analytics

### Detailed Architecture

The data flows through our system in the following pattern:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│   Data Sources  │────▶│  Data Ingestion │────▶│ Data Processing │
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│    Reporting    │◀────│   Data Access   │◀────│  Transformation │
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

#### Data Flow Details

| Stage | Components | Input | Process | Output |
|-------|------------|-------|---------|--------|
| **Data Sources** | NYC Open Data | Yellow & Green Taxi CSVs | - | Raw CSV files |
| **Data Ingestion** | Azure Data Factory | Raw CSV files | Extract & Load | Bronze layer (Raw) |
| **Data Processing** | Databricks Notebooks | Bronze layer data | Schema homogenization | Silver layer (Processed) |
| **Transformation** | Databricks SQL | Silver layer data | Join, aggregate, enrich | Gold layer (Curated) |
| **Data Access** | Databricks SQL Warehouse | Gold layer data | SQL queries | Query results |
| **Reporting** | Databricks Notebooks | Query results | SQL queries | Analysis results |

#### Storage Layer Details

| Layer | Format | Purpose | Example Tables |
|-------|--------|---------|---------------|
| **Bronze** | CSV | Raw data storage | yellow_taxi_trips_raw, green_taxi_trips_raw |
| **Silver** | Parquet/Delta | Processed data | taxi_zone_lookup (Parquet), yellow_taxi_trips_transform (Delta) |
| **Gold** | Delta Lake | Analytics-ready data | yellow_taxi_trips_transform, green_taxi_trips_transform |

## Setup

For a comprehensive setup guide, you can follow [module 01-Primer](https://github.com/microsoft/Azure-Databricks-NYC-Taxi-Workshop) in the original Azure-Databricks-NYC-Taxi-Workshop repository.

### Prerequisites

- Databricks workspace
- Azure Storage Account
- Python 3.11
- Databricks CLI configured

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

4. Set up Azure Storage credentials in Databricks secrets:
   - Create a secret scope named "azure-databricks"
   - Add secrets for "storage-name" and "storage-key"

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
| **Yellow Taxi Trips** | 2009-2017 | **1.37B** | ~300GB | ~120GB | **2.5:1** | Year, Month |
| **Green Taxi Trips** | 2013-2017 | **59M** | ~15GB | ~6GB | **2.5:1** | Year, Month |

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

## Results and Benchmarks

The project successfully processes and analyzes this data with the following performance metrics:

- Optimized Delta Lake tables with partitioning by year and month
- Query performance improvements:
  - 10-50x faster queries compared to raw CSV data
  - Efficient filtering on partitioned columns
  - Optimized joins with reference data

## Project Structure

The project follows a modular structure to separate different stages of the data pipeline:

```
Workspace/
├── 01-General/
│   └── 2-CommonFunctions.py         # Common utility functions used across notebooks
│
├── CarsProject/
│   ├── jupyter-notebook/            # Jupyter notebooks organized by function
│   │   ├── load-data/
│   │   │   ├── LoadDataGreenTaxi.ipynb     # Load Green Taxi data
│   │   │   ├── LoadDataYellowTaxi.ipynb    # Load Yellow Taxi data
│   │   │   └── LoadReferenceData.ipynb     # Load reference data
│   │   │
│   │   ├── transform-data/
│   │   │   ├── TransformData.ipynb              # General transformations
│   │   │   └── TransformDataYellowTaxiSpark.ipynb  # Spark transformations
│   │   │
│   │   └── analytics/
│   │       └── Report.ipynb         # Analysis and reporting
│   │
│   ├── databricks-notebook/         # Databricks version of notebooks
│   │
│   └── sql/                         # SQL transformations
│       └── transform/
│           ├── 1-transform-yellow-taxi.sql
│           ├── 2-transform-green-taxi.sql
│           └── 3-transform-create-materialize-view.sql
│
└── utilities/                       # Utility scripts
    ├── databricks_to_jupyter.py     # Convert Databricks to Jupyter format
    └── jupyter_to_databricks.py     # Convert Jupyter to Databricks format
```

Each notebook serves a specific purpose in the data pipeline, from ingestion to transformation to analysis.

## Notes

- The project handles different schema versions of NYC Taxi data across different years
- Data is stored in Delta format for reliability and performance
- Databricks notebooks can be converted to Jupyter notebooks for local development
- SQL transformations are version-controlled and can be synchronized with Databricks

## Future Enhancements

- **Implement Airflow Orchestration**: Automate and schedule Spark jobs and SQL transformations with Apache Airflow for improved workflow management
- **Adopt dbt for SQL Transformations**: Leverage dbt (data build tool) for better management, testing, and documentation of SQL transformation scripts
- **Establish CI/CD Pipeline**: Set up automated testing and deployment workflows to ensure code quality and streamline releases
- **Conduct Cloud Provider Comparison**: Evaluate Azure against other cloud platforms (AWS, GCP) for cost-effectiveness and performance benchmarks
