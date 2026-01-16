# MelPark - Melbourne Real-Time Parking Data Engineering Pipeline
![Azure](https://img.shields.io/badge/Cloud-Azure-blue)
![Terraform](https://img.shields.io/badge/IaC-Terraform-green)
![Data Lake](https://img.shields.io/badge/Data_Lake-ADLS_Gen2-blue)
![Databricks](https://img.shields.io/badge/Engine-Databricks-orange)
![Delta Lake](https://img.shields.io/badge/Storage-Delta_Lake-cyan)
![dbt](https://img.shields.io/badge/Transform-dbt_Core-orange)
![Airflow](https://img.shields.io/badge/Orchestration-Apache_Airflow-red)
![PowerBI](https://img.shields.io/badge/Visualization-Power_BI-yellow)

## Table of Contents

* [Problem Statement](https://www.google.com/search?q=%23problem-statement)
* [Overview](https://www.google.com/search?q=%23overview)
* [Tech Stack](https://www.google.com/search?q=%23tech-stack)
* [Project Architecture](https://www.google.com/search?q=%23project-architecture)
* [Project Structure](https://www.google.com/search?q=%23project-structure)
* [Component Deep Dive](https://www.google.com/search?q=%23component-deep-dive)
* [Data Source Overview](https://www.google.com/search?q=%23data-source-overview)
* [Data Extraction & Ingestion (Extract)](https://www.google.com/search?q=%231-data-extraction--ingestion-extract)
* [Real-Time Streaming with Kafka & Debezium](https://www.google.com/search?q=%232-real-time-streaming-with-kafka--debezium)
* [Lakehouse Ingestion with Spark & Azure (Load)](https://www.google.com/search?q=%233-lakehouse-ingestion-with-spark--azure-load)
* [Data Transformation with dbt (Silver to Gold)](https://www.google.com/search?q=%234-data-transformation-with-dbt-silver-to-gold)
* [Data Quality & Testing](https://www.google.com/search?q=%23data-quality--testing)
* [Insights & Visualizations](https://www.google.com/search?q=%23insights--visualizations)
* [Steps to Reproduce](https://www.google.com/search?q=%23steps-to-reproduce)
* [Conclusion & Future Enhancements](https://www.google.com/search?q=%23conclusion--future-enhancements)
* [Contact Information](https://www.google.com/search?q=%23contact-information)


## Problem Statement

Melbourne is one of the fastest-growing cities in Australia, and with this growth comes a significant challenge: urban congestion and parking scarcity. Finding a parking spot in the Melbourne CBD or inner suburbs like Carlton and Docklands often results in "circling the block," which increases carbon emissions, fuel consumption, and driver frustration. 

According to City of Melbourne data, thousands of sensors are embedded in the streets to monitor parking bay occupancy in real-time. However, for this data to be useful to drivers and urban planners, it needs to be processed, cleaned, and modeled at scale.

This project focuses on building a robust, automated data pipeline to ingest real-time sensor data from the **City of Melbourne Open Data Portal**. By transforming raw sensor events into a structured "Star Schema," we can provide insights into peak parking hours, average stay durations, and high-demand zones, ultimately helping to optimize urban mobility and travel planning.

## Overview

This pipeline implements a modern **Lakehouse Architecture** using a Medallion pattern (Bronze -> Silver -> Gold) to process real-time parking sensor data. 

The pipeline extracts data from the Melbourne Open Data API, ingests it into **Azure Data Lake Storage (ADLS) Gen2**, and uses **Azure Databricks** for high-performance processing. By leveraging **Structured Streaming**, the system captures parking events (Present/Unoccupied) as they happen.

Key highlights include:
- **Medallion Architecture:** Raw JSON ingestion (Bronze), cleaned Delta tables (Silver), and a business-ready Star Schema (Gold).
- **Unity Catalog:** Centralized governance and metadata management for all data assets.
- **dbt (Data Build Tool):** SQL-based modeling to transform Silver sensor events into analytical fact and dimension tables.
- **Infrastructure as Code:** The entire Azure environment (Databricks, Storage, Key Vault) is provisioned via **Terraform**.
- **Orchestration:** Managed workflows ensuring data flows seamlessly from the API to the final Power BI dashboard.


For your **Tech Stack** section, we’ll use a combination of professional badges and deep-dive descriptions. This shows recruiters not just *what* you used, but *why* you chose them for a high-scale parking project.

## Tech Stack

<img src="https://cdn.simpleicons.org/docker" height="28"/> <img src="https://cdn.simpleicons.org/apachekafka" height="28"/> <img src="https://cdn.simpleicons.org/apacheflink" height="28"/> <img src="https://cdn.simpleicons.org/apacheairflow" height="28"/> <img src="https://debezium.io/assets/images/color_white_debezium_type_600px.svg" height="28"/> <img src="https://upload.wikimedia.org/wikipedia/commons/a/a8/Microsoft_Azure_Logo.svg" height="28"/> <img src="https://cdn.simpleicons.org/terraform" height="28"/> <img src="https://cdn.simpleicons.org/databricks" height="28"/> <img src="https://cdn.simpleicons.org/delta" height="28"/> <img src="https://assets.streamlinehq.com/image/private/w_34,h_34,ar_1/f_auto/v1/icons/logos/dbt-5ag473n5ue7nr8spde8xya.png/dbt-j1jrw7mrg7avpsf5al73ls.png?_a=DATAiZfmZAA0" height="28"/>

## Tech Stack

* **Containerization & Deployment:** **Docker & Docker Compose**
Standardizes the environment for local development and testing of Kafka and Flink components, ensuring "it works on my machine" consistency before deploying to Azure.
* **Change Data Capture (CDC):** **Debezium**
Used to monitor source databases and stream row-level changes into Kafka. This ensures that every parking status update is captured with zero lag from the source.
* **Event Streaming Platform:** **Apache Kafka**
The central nervous system of the pipeline. It handles high-throughput real-time message ingestion, decoupling the source APIs/Databases from our downstream processing.
* **Stream Processing:** **Apache Flink**
Performs stateful computations over data streams. Flink is utilized for real-time aggregations and windowing (e.g., calculating immediate occupancy percentages) before data hits the Lakehouse.
* **Orchestration:** **Apache Airflow**
Schedules and monitors the end-to-end data flow. It coordinates the extraction from external APIs and triggers dbt models once data has landed in the Silver layer.
* **Cloud Platform & Infrastructure:** **Azure & Terraform**
**Terraform** provisions the **Azure Data Lake (ADLS Gen2)**, **Databricks**, and **Key Vault**. This "Infrastructure as Code" approach ensures the cloud environment is secure and reproducible.
* **Lakehouse Processing:** **Databricks & Delta Lake**
Implements the Medallion Architecture. **Spark Structured Streaming** ingests Kafka topics into Delta tables, providing ACID transactions for our streaming data.
* **Data Transformation & Modeling:** **dbt Core**
Transforms Silver Delta tables into the Gold Star Schema. It handles the complex logic for sessionization and ensures data quality via automated tests.


## Project Architecture

<img width="4125" height="4071" alt="data_model - Page 13 (1)" src="https://github.com/user-attachments/assets/ff4c0420-c455-4293-9128-bfea3d28dda4" />

The MelPark pipeline utilizes a **Real-Time Lakehouse Architecture** to transform raw sensor events from the City of Melbourne into curated business insights. The system is designed to handle high-velocity data through a decoupled, streaming-first approach.

The data flow is divided into four primary stages:

### 1. Extract

* **Data Source**: Real-time parking occupancy data is ingested from the **City of Melbourne API**.
* **Transactional Layer**: Data is initially pushed to a **PostgreSQL** database for transactional workloads.
* **Change Data Capture (CDC)**: **Debezium** monitors the PostgreSQL logs to instantly capture row-level changes (inserts/updates), ensuring zero-lag synchronization between the database and the streaming layer.

### 2. Load

* **Stream Hub**: Captured events are streamed into **Apache Kafka**, which acts as a high-throughput distributed message broker. Kafka decouples the ingestion source from downstream storage, providing a resilient buffer for the live sensor stream.
* **Storage Sink**: Data is consumed from Kafka and landed into **Azure Data Lake Storage (ADLS Gen2)**. This serves as the **Delta Lake Storage** layer, maintaining data in its raw form before processing.

### 3. Transform (Medallion Architecture)

Data is processed within **Azure Databricks** using a tiered Medallion pattern:

* **Bronze**: Raw data landing from the stream.
* **Silver (Data Wrangling and Cleaning)**: Spark jobs perform schema enforcement, data deduplication, and cleaning.
* **Gold (Data Modelling)**: **dbt (data build tool)** is utilized to model the cleaned data into a high-performance Star Schema, optimized for analytical queries.

### 4. Visualize

* **Analytics**: The final curated datasets are exposed via a Databricks SQL Warehouse.
* **Real-Time Visualization**: **Power BI** connects to the Gold layer to provide interactive geographic heatmaps, occupancy trends, and duration analysis.

### Supporting Infrastructure

* **Workflow Orchestration**: **Apache Airflow** serves as the primary orchestrator, managing task dependencies and scheduling for the entire stream processing lifecycle.
* **Containerisation**: The local development and streaming components (Kafka, Debezium, Postgres) are managed using **Docker** to ensure environment consistency.
* **Infrastructure as Code (IaC)**: The entire Azure environment, including storage accounts, Key Vaults, and Databricks workspaces, is provisioned and managed via **Terraform**.

Based on your current project layout shown in your file explorer, here is a clean and professional **Project Structure** section for your `README.md`.

I have organized this to reflect the specialized folders you've created for **streaming**, **orchestration**, and the **medallion architecture** (Bronze/Silver/Gold).

---

## Project Structure

```text
melbourne_parking_pipeline/
├── .github/workflows/        # CI/CD pipelines (ci.yml)
├── databricks/               # Medallion Architecture notebooks
│   ├── Bronze.ipynb          # Raw data ingestion
│   └── Silver.ipynb          # Data cleaning and transformation
├── dbt_gold/                 # Gold Layer transformations (Star Schema)
│   ├── models/               # dbt SQL models
│   ├── tests/                # Data quality tests
│   └── dbt_project.yml       # dbt configuration
├── ingestion/                # External data fetching
│   └── parking_api.py        # City of Melbourne API client
├── orchestration/            # Workflow management
│   ├── dags/                 # Airflow Directed Acyclic Graphs
│   ├── docker-compose.yaml   # Airflow stack definition
│   └── Dockerfile            # Custom Airflow image with dependencies
├── processing/               # Real-time streaming components
│   ├── docker-compose.yml    # Kafka, Zookeeper, and Debezium stack
│   ├── postgres_loader.py    # Mock producer for transactional data
│   ├── setup_debezium.py     # CDC connector configuration
│   └── spark_loader.py       # Spark streaming ingestion logic
├── terraform/                # Infrastructure as Code
│   ├── main.tf               # Azure and Databricks resources
│   ├── variables.tf          # Configurable environment variables
│   └── terraform.tfvars      # Secret/local variable values
├── spark-jars/               # Required libraries for Azure/Postgres connectivity
├── Makefile                  # Shortcuts for common commands (make setup, etc.)
├── setup.py                  # Project packaging and dependencies
└── README.md                 # Project documentation

```

### Folder Highlights

* **`processing/`**: Contains the core real-time streaming logic, including the **Kafka** and **Debezium** configurations used for Change Data Capture (CDC).
* **`dbt_gold/`**: Houses the final analytical layer where raw sensor data is modeled into business-ready dimensions and facts.
* **`terraform/`**: Fully automates the deployment of the Azure environment, including the **ADLS Gen2** storage containers and **Databricks** workspace.
* **`orchestration/`**: Centralizes the pipeline management using **Apache Airflow**, ensuring seamless data flow from API to Dashboard.

To align with your **Azure, Databricks, and Kafka** stack, we need to adapt the component descriptions to reflect your specific files (like `setup_debezium.py` and `spark_loader.py`) and the **Medallion Architecture**.


## Component Deep Dive

### 1. Infrastructure as Code (Terraform)

Automates the deployment of the Azure environment to ensure 100% reproducibility.

* **`main.tf`**: Defines core Azure resources including the **ADLS Gen2** data lake, **Databricks** workspace, and **Azure Key Vault**.
* **`providers.tf`**: Configures the `azurerm` and `databricks` providers with specific version pinning.
* **`variables.tf`**: Parameterizes the environment (e.g., `project_name`, `location`) for reuse across Dev/Prod stages.
* **`outputs.tf`**: Exports critical endpoints like the Databricks Workspace URL and Storage Account names for use in other services.

### 2. Real-Time Ingestion (Extract)

Handles the transition from the City of Melbourne API to a streaming event log.

* **`ingestion/parking_api.py`**: Interacts with the Melbourne Open Data API to fetch real-time sensor occupancy.
* **`processing/postgres_loader.py`**: Simulates transactional workloads by loading raw sensor data into **PostgreSQL**.
* **`processing/setup_debezium.py`**: Configures the **CDC (Change Data Capture)** connector to stream row-level changes from Postgres into Kafka.
* **`tests/postman/`**: Contains API collections to validate endpoint connectivity and response schemas.

### 3. Stream Processing & Loading (Processing)

Bridges the gap between Kafka topics and the Delta Lakehouse.

* **`processing/spark_loader.py`**: A Spark Structured Streaming application that consumes Kafka topics and sinks them into the **Bronze** layer.
* **`spark-jars/`**: Provides essential drivers (Hadoop-Azure, Postgres) to enable connectivity between Spark and the Azure/Kafka ecosystem.
* **`processing/docker-compose.yml`**: Manages the local lifecycle of the streaming cluster (**Kafka, Zookeeper, and Debezium**).

### 4. Medallion Architecture (Transform)

Orchestrates the data quality journey from raw events to analytical insights.

* **Databricks Notebooks (`Bronze.ipynb`, `Silver.ipynb`)**:
* **Bronze**: Ingests raw streams using **Autoloader** for efficient file discovery.
* **Silver**: Implements schema enforcement, data deduplication, and standardization.


* **dbt Gold Layer (`dbt_gold/`)**:
* **Staging**: Cleans and standardizes Silver Delta tables.
* **Marts**: Implements **Star Schema** logic, creating `fact_parking_sessions` and dimension tables (e.g., `dim_bays`).
* **`dbt_project.yml`**: Manages the build hierarchy and transformation configurations.


<img width="604" height="310" alt="image" src="https://github.com/user-attachments/assets/17b85e37-0ee8-4924-af3c-15a7e233efa6" />

<img width="658" height="325" alt="image" src="https://github.com/user-attachments/assets/5473246a-a430-4890-8897-ead4a9782450" />



### 5. Orchestration (Airflow)

The "control plane" for the entire pipeline.

* **`orchestration/dags/`**: Defines the workflow logic to trigger API extraction, streaming jobs, and dbt models in the correct sequence.
* **`orchestration/Dockerfile`**: A customized Airflow image containing the Azure CLI and Databricks provider plugins.
<img width="1256" height="622" alt="image" src="https://github.com/user-attachments/assets/311e11b6-8e3f-45b3-85da-5eddd435a6bb" />
(Dag that ingests and loads data to both postgresql and azure container)

<img width="1191" height="525" alt="image" src="https://github.com/user-attachments/assets/65a1c186-74c5-47a3-819e-07de2a493173" />
(Dag that orchestrates the medallion pipeline)

## Data Source Overview

The **City of Melbourne Open Data Portal** is a leading municipal data platform that provides researchers and developers with access to a vast array of urban datasets. These APIs offer real-time insights into city life, including pedestrian counts, traffic flow, and environmental monitoring, which are essential for building smart city applications.

This project specifically utilizes the **On-street Parking Bay Sensors API**, which provides real-time occupancy status for over 4,000 parking sensors embedded in the city's streets.

* **Update Frequency**: The sensors update every few minutes to provide the most current "Present" or "Unoccupied" status of each parking bay.
* **Data Scope**: The API covers major areas including the CBD, Carlton, and Docklands, capturing data for car, motorcycle, and heavy vehicle bays.
* **Accessibility**: Access to this data is provided through an open API key system, allowing for seamless integration into large-scale data engineering pipelines.
* **Project Value**: By capturing these real-time events through **Debezium** and **Kafka**, this project transforms volatile sensor pings into a historical record of urban mobility, providing insights that are not available through the raw API alone.

<img width="1264" height="973" alt="image" src="https://github.com/user-attachments/assets/2338b517-e526-4978-9bcc-7fa6088c314f" />

For more details, please refer to the [API documentation](https://data.melbourne.vic.gov.au/explore/dataset/on-street-parking-bay-sensors/information/?dataChart=eyJxdWVyaWVzIjpbeyJjaGFydHMiOlt7InR5cGUiOiJjb2x1bW4iLCJmdW5jIjoiQ09VTlQiLCJ5QXhpcyI6ImJheV9pZCIsInNjaWVudGlmaWNEaXNwbGF5Ijp0cnVlLCJjb2xvciI6IiNFNTBFNTYifV0sInhBeGlzIjoibGFzdHVwZGF0ZWQiLCJtYXhwb2ludHMiOjUwLCJzb3J0IjoiIiwidGltZXNjYWxlIjoiZGF5IiwiY29uZmlnIjp7ImRhdGFzZXQiOiJvbi1zdHJlZXQtcGFya2luZy1iYXktc2Vuc29ycyIsIm9wdGlvbnMiOnt9fX1dLCJ0aW1lc2NhbGUiOiIiLCJkaXNwbGF5TGVnZW5kIjp0cnVlLCJhbGlnbk1vbnRoIjp0cnVlfQ%3D%3D).


### 1. Data Extraction & Ingestion (Extract)

The extraction layer is responsible for the initial retrieval of volatile sensor data and its transition into a durable transactional state. This stage ensures that we capture the state of Melbourne's 4,000+ parking sensors before the real-time API refreshes.

#### API Data Retrieval

* **Technical Implementation**: A custom Python client manages the interface with the **City of Melbourne Open Data Portal**.
* **Project File**: `ingestion/parking_api.py`.
* **Logic & Execution**:
* **Authentication**: Handles API key headers and request session management to the Melbourne Data Portal.
* **Polling Strategy**: Orchestrated by **Airflow** to poll the API every few minutes, matching the sensor's physical refresh rate.
* **Resilience**: Implements error handling for API rate limits and connection timeouts to prevent data gaps.

<img width="1469" height="797" alt="image" src="https://github.com/user-attachments/assets/8a555553-715d-400d-9a31-0887f383ef36" />


#### Transactional Loading

* **Transactional Layer**: Fetched JSON data is immediately loaded into a **PostgreSQL** database.
* **Project File**: `processing/postgres_loader.py`.
* **Logic**:
* **Row-Level Storage**: Data is stored in a structured relational format, serving as the "System of Record" for the transactional side of the house.
* **Change Trigger**: By writing to Postgres, we create the necessary database logs (WAL) that allow for downstream **Change Data Capture (CDC)**.

<img width="1152" height="798" alt="image" src="https://github.com/user-attachments/assets/e6525978-65c7-4f47-9786-5110c0d9998b" />


#### Validation & Testing

* **Project Files**: `tests/postman/parking_api_collection.json` and `extraction/test_api.py`.
* **Verification**:
* **API Contract**: Postman collections validate that the API response matches the expected schema (Bay ID, Status, Lat/Lon).
* **Connectivity**: `test_api.py` ensures the local environment has the correct network permissions to reach Azure and external API endpoints.

### 2. Real-Time Streaming with Kafka & Debezium

This stage transforms static database records into a continuous event stream, enabling the "Real-Time" capabilities of the MelPark pipeline. Instead of traditional batch polling, we use **Change Data Capture (CDC)** to react to every parking sensor update as it happens.

#### Change Data Capture (CDC)

* **Technical Implementation**: **Debezium** acts as a low-latency connector between the transactional database and the message broker.
* **Project File**: `processing/setup_debezium.py`.
* **Logic & Execution**:
* **Log Monitoring**: Debezium reads the **PostgreSQL Write-Ahead Log (WAL)** to detect row-level changes.
* **Event Generation**: Every time a parking bay status changes (e.g., from "Unoccupied" to "Present"), Debezium generates a structured JSON event representing the *before* and *after* state.
* **Zero-Lag**: This method avoids puting unnecessary load on the Postgres database compared to traditional SQL queries.



#### Event Streaming & Buffering

* **Technical Implementation**: **Apache Kafka** serves as the central distributed message broker.
* **Project File**: `processing/docker-compose.yml`.
* **Logic & Execution**:
* **Topic Partitioning**: Data is organized into Kafka topics (e.g., `melbourne.parking.sensors`), allowing for high-throughput and parallel processing.
* **Persistence & Decoupling**: Kafka stores the sensor events temporarily, acting as a buffer that decouples the fast ingestion from the downstream loading process.
* **Fault Tolerance**: If the downstream Azure storage is temporarily unavailable, Kafka retains the messages to ensure no sensor data is lost during the downtime.



#### Connector Orchestration

* **Technical Implementation**: The entire streaming cluster is containerized for consistency across environments.
* **Project File**: `processing/docker-compose.yml`.
* **Logic**:
* **Zookeeper**: Manages the Kafka cluster state and configuration.
* **Kafka Connect**: Hosts the Debezium engine and manages the lifecycle of the Postgres source connector.

### 3. Lakehouse Ingestion with Spark & Azure (Load)

This stage facilitates the movement of data from the transient Kafka event stream into the permanent storage of the **Azure Data Lake (ADLS Gen2)**. By utilizing the **Medallion Architecture**, we ensure that data is stored in its rawest form before any transformations occur.

#### Spark Structured Streaming

* **Technical Implementation**: A Spark application acting as a Kafka consumer to pull real-time events.
* **Project File**: `processing/spark_loader.py`.
* **Logic & Execution**:
* **Stream Consumption**: Spark connects to the Kafka broker defined in the Docker environment and subscribes to the sensor topics.
* **Trigger Strategy**: Utilizes the `availableNow=True` trigger to process all available data from the stream incrementally, providing a cost-effective alternative to continuous 24/7 streaming.
* **Checkpointing**: Implements checkpointing to ensure "exactly-once" processing semantics, allowing the pipeline to resume from the last successful write in case of failure.



#### Bronze Layer Landing

* **Technical Implementation**: Data is written to **Azure Data Lake Storage (ADLS Gen2)** using the **Delta Lake** format.
* **Project File**: `databricks/Bronze.ipynb`.
* **Logic & Execution**:
* **Autoloader**: Databricks Autoloader (`cloudFiles`) is used to detect and ingest new files arriving in the storage container with minimal configuration.
* **Schema Evolution**: Delta Lake provides schema enforcement and evolution, ensuring that if the Melbourne API adds new fields, the pipeline doesn't break.
* **Historical Archive**: The Bronze layer stores the raw JSON-to-Delta conversion, serving as a single source of truth for all downstream reprocessing.

<img width="1275" height="672" alt="image" src="https://github.com/user-attachments/assets/5163831b-00e7-4416-9cab-439ad0bde823" />


#### Connectivity & JAR Management

* **Technical Implementation**: Managed Spark dependencies for cloud and database integration.
* **Project Folder**: `spark-jars/`.
* **Logic**:
* **Storage Drivers**: Includes `hadoop-azure` and `azure-storage` JARs to allow Spark to communicate natively with ADLS Gen2.
* **Database Drivers**: Includes `postgresql-42.7.2.jar` to facilitate potential batch reads from the transactional source.

<img width="758" height="223" alt="image" src="https://github.com/user-attachments/assets/abc51e82-98e2-4216-ab3f-60c96cc187e4" />

### 4. Data Transformation with dbt (Silver to Gold)

This final stage of the pipeline transitions data from raw sensor events into high-value analytical models. By utilizing **dbt Core** on top of **Azure Databricks**, we implement a structured transformation layer that ensures data quality, consistency, and a clear lineage from the streets of Melbourne to the final dashboard.

#### Silver Layer: Cleaning & Standardization

* **Technical Implementation**: Spark-based transformation jobs.
* **Project File**: `databricks/Silver.ipynb`.
* **Logic & Execution**:
* **Schema Enforcement**: Casts raw strings into proper data types (e.g., converting ISO strings to timestamps and sensor status to Booleans).
* **Deduplication**: Uses Spark's `dropDuplicates` based on the unique Bay ID and Last Updated timestamp to ensure each event is recorded exactly once.
* **Normalization**: Standardizes bay descriptions and status values (e.g., mapping "Present" to 1 and "Unoccupied" to 0) for easier aggregation.

<img width="1623" height="855" alt="image" src="https://github.com/user-attachments/assets/0b98c295-cde0-44b0-a903-cf0f3960a1ae" />


#### Gold Layer: Analytical Modeling

* **Technical Implementation**: **dbt Core** running on the Databricks SQL Warehouse.
* **Project Folder**: `dbt_gold/`.
* **Logic & Execution**:
* **Dimension Tables**: Models like `dim_bays` and `dim_locations` are created to store descriptive information about the parking infrastructure.
* **Fact Tables**: The `fact_parking_sessions` table is built by calculating stay durations through complex SQL window functions that "stitch" sensor pings together.
* **Star Schema**: dbt organizes these tables into a Star Schema, which is optimized for high-performance BI queries in Power BI.

<img width="1211" height="968" alt="image" src="https://github.com/user-attachments/assets/e70432f5-563d-4366-a4ad-a213234dca05" />

<img width="1275" height="780" alt="image" src="https://github.com/user-attachments/assets/9a1826e6-3235-4a58-8d67-eec06ff5b5ab" />



### Data Quality & Testing

This project implements a multi-layered testing strategy to ensure data reliability, from the initial API handshake to the final analytical transformations. By catching errors early in the pipeline, we maintain high trust in the parking occupancy metrics.

#### 1. Unit & Connectivity Testing

Before data enters the streaming pipeline, we validate our connection to external and cloud resources.

* **API Validation**: `extraction/test_api.py` checks the validity of the City of Melbourne API key and verifies that the response structure (headers and JSON body) aligns with our expected schema.
* **Producer Testing**: `extraction/test_kafka.py` ensures that our Python scripts can successfully produce messages to the Kafka broker within the Docker environment.
* **API Collections**: The `tests/postman/parking_api_collection.json` file provides a suite of manual and automated tests to verify the endpoint's health and latency independent of our Python code.

#### 2. Integration & Stream Testing

We monitor the collaborative capability between our streaming components.

* **Connector Status**: We use the Kafka Connect REST API (via `curl`) to monitor the health of the **Debezium** connector and ensure it is actively reading the Postgres WAL logs.
* **Consumer Validation**: We perform manual "sink tests" by using the Kafka console consumer to verify that row-level changes from Postgres are being correctly serialized into JSON events on the Kafka topic.

#### 3. dbt Testing Framework

The final and most rigorous testing occurs during the Gold transformation layer using **dbt Core**.

* **Generic Tests**: We apply `not_null` and `unique` constraints to primary keys like `bay_id` and `session_id` to prevent data duplication or missing records.
* **Relationship Tests**: We enforce referential integrity between the `fact_parking_sessions` and dimension tables like `dim_bays` to ensure every session is linked to a valid parking location.
* **Custom Data Tests**: Custom SQL tests are implemented to find "impossible" data, such as parking sessions with negative durations or occupancy counts that exceed the bay's capacity.

#### 4. Infrastructure Validation

* **Terraform Plan**: Before any cloud changes are applied, we run `terraform plan` to validate the infrastructure state and prevent accidental resource destruction.
* **CI/CD**: The `.github/workflows/ci.yml` file automates basic linting and syntax checks whenever code is pushed to the repository.

## Insights & Visualizations

The MelPark dashboard is an end-to-end business intelligence solution that provides a "consolidated view" of Melbourne's urban mobility. By transforming raw sensor pings into "visualized facts," it enables city managers to move from basic reporting to "operational optimization".

<img width="1505" height="845" alt="image" src="https://github.com/user-attachments/assets/8f250606-ef6e-43ec-aff9-592b8de46c3e" />

### 1. Interactive Real-Time Map

The centerpiece of the dashboard is a high-fidelity geographic visualization that acts as a "digital twin" of the city.

* **Live Occupancy Styling**: Utilizing data-driven styling, each bay is color-coded to represent real-time status: **Purple/Red** for "Present" and **Dark/Green** for "Unoccupied".
* **Spatial Granularity**: Visualizes over 4,000 in-ground sensors mapped via precise Latitude and Longitude coordinates from the **On-street Parking Bays** dataset.
* **Dynamic Slicing**: Users can filter the map by **Bay ID** or **Stay Category** to isolate specific zones, such as "1P Meter" or "Disabled Only" bays.

### 2. Key Performance Indicators (KPIs)

The dashboard tracks critical metrics to measure "progress toward measurable goals" for urban accessibility.

* **Occupancy Accumulation**: Tracks the number of vehicles parked at any given moment, providing a live snapshot of city density.
* **Stay Duration (Minutes)**: A "Total Duration" bar chart aggregates the time spent in bays, segmented by restriction type (e.g., 1P, 2P).
* **Revenue Correlation**: Integrates infrastructure data to track "Actual Revenue" against occupancy, highlighting the financial performance of different parking zones.
* **Infrastructure Health**: Monitors the percentage of bays with "Has Credit Card?" capabilities, ensuring payment accessibility for short-term visitors.

### 3. Temporal & Behavioral Trends

By scrutinizing historical trends, the dashboard assists in "forecasting demand" for future city planning.

* **Occupancy Over Time**: A line/area chart (e.g., "Sum of duration_minutes by Day") identifies seasonality and "peak demand periods," such as weekends or festive seasons.
* **Turnover Analysis**: Measures how many unique vehicles utilize a single space over a period, a key indicator of economic activity and "resource optimization".
* **Overstay Tracking**: Visualizes compliance by flagging records where a vehicle has "overstayed the parking restriction" for its bay.

### 4. Technical Dashboard Design

* **DirectQuery Connection**: The report maintains a live connection to the **Databricks SQL Warehouse**, ensuring that visuals update as soon as new events are processed by the Spark stream.
* **DAX Measures**: Custom measures are used to calculate complex logic, such as "estimated vs. actual" occupancy and time-based variance metrics.
* **Conditional Formatting**: Visual cues (e.g., color scales or icons) instantly spot "performance gaps" or areas with frequent parking violations.

By centralizing these "disjointed spreadsheets" into a dynamic platform, the MelPark dashboard drives "timely decision-making" for Melbourne's future urban development.

## Steps to Reproduce

This section provides a detailed guide to deploying the MelPark pipeline, from provisioning cloud infrastructure to launching the real-time streaming cluster.

### 1. Prerequisites & Environment Setup

Before beginning, ensure your local environment meets the following requirements:

* **Azure Subscription**: A valid account with "Contributor" access to create resources.
* **Local Tools**: Install **Terraform**, **Docker Desktop**, **Azure CLI**, and **Python 3.9+**.
* **API Access**: Register for an API Key at the [City of Melbourne Open Data Portal](https://data.melbourne.vic.gov.au/).

### 2. Infrastructure as Code (Terraform)

Provision the core Azure and Databricks components:

1. Navigate to the terraform directory: `cd terraform`.
2. Initialize the backend and providers: `terraform init`.
3. Create a `terraform.tfvars` file and define your specific project variables.
4. Deploy the infrastructure:
```bash
terraform plan
terraform apply -auto-approve

```


5. **Note**: The output will provide your **ADLS Storage Account Name** and **Databricks Workspace URL** required for subsequent steps.

### 3. Local Streaming Cluster (Docker & Debezium)

Launch the containerized stack to enable Change Data Capture (CDC):

1. Move to the processing folder: `cd ../processing`.
2. Start the services (Kafka, Zookeeper, Postgres, Debezium):
```bash
docker-compose up -d

```


3. Initialize the Debezium connector to start monitoring the Postgres WAL:
```bash
python setup_debezium.py

```


4. Verify the connector is active:
```bash
curl -H "Accept:application/json" localhost:8083/connectors/melpark-connector/status

```



### 4. Databricks & dbt Configuration

Set up the Lakehouse transformation layers:

1. **Bronze & Silver**: Upload the notebooks from the `databricks/` folder to your workspace and run `Bronze.ipynb` to start the Autoloader stream.
2. **dbt Profiles**: Configure your `~/.dbt/profiles.yml` with the Databricks SQL Warehouse connection details (Host, HTTP Path, and Personal Access Token).
3. **Run Models**:
```bash
cd ../dbt_gold
dbt deps
dbt run
dbt test

```



### 5. Orchestration (Airflow)

Deploy the Airflow scheduler to manage the end-to-end data flow:

1. Launch Airflow via Docker:
```bash
cd ../orchestration
docker-compose up -d

```


2. Access the UI at `localhost:8080` and trigger the `melpark_ingestion_dag`.

### 6. Power BI Connection

1. Open the `.pbix` file located in the project root.
2. Under **Transform Data > Data Source Settings**, update the server details to match your Databricks SQL Warehouse.
3. Click **Refresh** to populate the dashboard with your live parking data.

## Conclusion & Future Enhancements

The MelPark project demonstrates a scalable, real-time approach to urban data challenges. By combining **Change Data Capture (CDC)** with a **Medallion Lakehouse architecture**, we move beyond simple batch processing to provide immediate, actionable insights into Melbourne's parking ecosystem.

### Future Enhancements

* **Machine Learning Integration**: Implement predictive models in Databricks to forecast parking availability 30-60 minutes in advance.
* **Real-Time Alerting**: Use Airflow or Azure Functions to trigger notifications when specific zones reach critical occupancy levels.
* **Flink Stream Processing**: Shift complex windowing logic from Spark to **Apache Flink** for even lower latency on streaming aggregations.

---

## Contact Information

Feel free to reach out if you have any questions about the architecture, the tech stack, or potential collaborations!

* **Name:** Avirukth Thadaklur
* **Email:** [avirukth@gmail.com](mailto:avirukth@gmail.com)
* **LinkedIn:** [linkedin.com/in/avirukth-thadaklur/](https://www.linkedin.com/in/avirukth-thadaklur/)
* **Project Portfolio:** [GitHub Profile](https://www.google.com/search?q=https://github.com/avirukthgt)
