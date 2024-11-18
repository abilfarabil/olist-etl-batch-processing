# Olist ETL Batch Processing

## Overview
This project implements an ETL (Extract, Transform, Load) batch processing pipeline for Olist e-commerce data using Apache Airflow, Apache Spark, and PostgreSQL. The pipeline extracts data from CSV files, loads it into PostgreSQL, and performs sales performance analysis based on geographical regions.

## System Architecture
![Containers](images/docker-containers.png)

The project utilizes three main components:
- **Apache Airflow**: Workflow orchestration
- **Apache Spark**: Data processing engine
- **PostgreSQL**: Data storage and analysis

## Prerequisites
- Docker & Docker Compose
- Make
- Python 3.8+
- Minimum 8GB RAM recommended

## Installation & Setup
1. Clone repository
```bash
git clone https://github.com/abilfarabil/olist-etl-batch-processing.git
cd olist-etl-batch-processing
```

2. Start services
```bash
make postgres
make spark
make airflow
```

3. Access Airflow UI
- URL: `localhost:8081`
- Username: airflow
- Password: airflow

## Pipeline Components

### 1. ETL Process
The ETL process extracts data from Olist CSV files and loads them into PostgreSQL tables.

#### Airflow DAG View
![ETL DAG List](images/etl-dag-list.png)

#### ETL DAG Graph
![ETL DAG Graph](images/etl-dag-graph.png)

#### ETL Success Status
![ETL Success](images/etl-dag-success.png)

#### Data Verification
![ETL Postgres Verification](images/etl-postgres-verify.png)

### 2. Analysis Process
The analysis process reads data from PostgreSQL and performs sales performance analysis by region.

#### Analysis DAG Graph
![Analysis DAG Graph](images/analysis-dag-graph.png)

#### Analysis Success Status
![Analysis Success](images/analysis-dag-success.png)

#### Analysis Results
![Analysis Results](images/analysis-results.png)

## Features
- Automated ETL pipeline using Apache Airflow
- Scalable data processing with Apache Spark
- Data storage and analysis in PostgreSQL
- Containerized environment with Docker
- Sales performance analysis by region
- Data quality checks and verification

## Technical Documentation
### ETL Pipeline
- Extracts data from Olist CSV files
- Performs data cleaning and transformation
- Loads processed data into PostgreSQL
- Includes data validation steps

### Analysis Pipeline
- Calculates regional sales metrics
- Generates performance reports
- Exports results to specified format

## Technologies Used
- Apache Airflow 2.x
- Apache Spark 3.x
- PostgreSQL 13
- Python 3.8+
- Docker & Docker Compose
- Make

## References
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Olist Dataset](https://www.kaggle.com/olist/brazilian-ecommerce)

## Contributing
Feel free to submit issues, fork the repository, and create pull requests for any improvements.
