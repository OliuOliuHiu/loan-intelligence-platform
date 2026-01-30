# DATA_VISUALIZATION PROJECT

## Overview

The **Data Visualization Project** is designed to extract, process, and visualize data from multiple sources such as SQL Server and REST APIs.

It follows a fully automated ELT (Extract – Load – Transform) pipeline, orchestrated by Apache Airflow, with dbt transformations triggered directly within the DAG.

All components are containerized and managed using Docker Compose for easy deployment and consistent environments.

---

## Technologies Used

- **Apache Airflow** – Workflow orchestration and ETL automation  
- **dbt (Data Build Tool)** – Data transformation & model building  
- **PostgreSQL** – Data Mart  
- **pgAdmin** – UI tool to monitor/query PostgreSQL  
- **SQL Server** – Raw data source (restored from `.bak`)  
- **Docker Compose** – Container orchestration  
- **`.env`** – Store all configuration variables and credentials  

---

## Data Pipeline Architecture

Below is a high-level architecture of the ELT process and orchestration:

![Data Pipeline](pipelineE2E.png)

> Extract from SQL Server and REST API → Load to PostgreSQL → Transform with dbt → Visualize in Power BI  
> All orchestrated by Apache Airflow and containerized with Docker.

---

## Project Structure

```
DATA_VISUALIZATION/
├── airflow/                  # Airflow-related configs and DAGs
│   ├── config/              # Airflow configuration files
│   ├── dags/                # DAG definitions
│   │   └── etl_multi_source.py
│   ├── logs/                # Airflow logs
│   ├── plugins/             # Optional custom plugins
│   └── Dockerfile           # Airflow service image
│
├── backup/                  # SQL Server backup files (.bak)
│
├── dbt_project/
│   ├── dbt_packages/        # Package dependencies
│   ├── logs/                # dbt logs
│   ├── macros/              # Custom dbt macros
│   ├── models/
│   │   ├── marts/           # Fact/dimension models
│   │   └── staging/         # Staging models
│   ├── profiles/
│   │   ├── .user.yml
│   │   └── profiles.yml
│   ├── target/              # Compiled dbt artifacts
│   ├── dbt_project.yml      # Project config
│   └── package-lock.yml
│
├── dashboard_powerbi
│   ├── preview image dashboard
│   ├── loan_dashboard.pbix  # dashboard powerbi file
│
├── .env                     # Environment config (generated from .env.example)
├── .env.example             # Template for environment variables
├── .gitignore               # Files to ignore in Git
├── docker-compose.yml       # Docker Compose configuration
└── README.md                # This documentation file
```

---
