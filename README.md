# Spark Bulk Data Load (SBDL)
This repository demonstrates how to build an Apache Spark-based bulk data processing pipeline that extracts entity data from Hive tables, processes and transforms it, and finally publishes the data to Apache Kafka.

This project is designed to handle large-scale entity data ingestion from an MDM (Master Data Management) Platform and follows a modular, scalable, and reusable architecture.

## **Problem Statement**
Organizations managing large-scale entity data often struggle with:

- **Efficient Data Extraction**: Retrieving and transforming entity records from structured sources like **Hive**.
- **Processing Complexity**: Combining **accounts, parties, and addresses** into a unified format.
- **Streaming & Integration**: Publishing transformed data to **Kafka** for downstream consumption.

This project addresses these challenges by implementing a **high-performance Spark application** that:
1. **Reads entity data from Hive tables** for a specified **load date**.
2. **Applies transformations** to join related entity records.
3. **Publishes the processed records to Kafka**.

## **System Architecture**
The architecture consists of:
1. **Data Ingestion**: Entity data is stored in **Hive** tables.
2. **Processing Engine**: **Apache Spark** loads, processes, and prepares data.
3. **Streaming Output**: **Kafka** is used as the streaming destination.



## **Datasets**
We process three key datasets: **Accounts, Parties, and Addresses**, which contain structured entity information.

### **Accounts Table**
| load_date | account_id  | legal_title_1 | legal_title_2 | tax_id | country |
|-----------|------------|---------------|---------------|--------|---------|
| 02-08-2022 | 6982391060 | Tiffany Riley | Matthew Davies | EIN | Mexico |
| 02-08-2022 | 6982391061 | Garcia and Sons | Taylor Guzman | SSP | USA |

### **Parties Table**
| load_date | account_id  | party_id  | relation_type | relation_start_date |
|-----------|------------|-----------|---------------|---------------------|
| 02-08-2022 | 6982391060 | 9823462810 | F-N | 2019-07-29 |

### **Party Address Table**
| load_date | party_id  | address_line_1 | city  | postal_code | country |
|-----------|----------|---------------|-------|-------------|---------|
| 02-08-2022 | 9823462810 | 45229 Drake Route | Shanefort | 77163 | Canada |













