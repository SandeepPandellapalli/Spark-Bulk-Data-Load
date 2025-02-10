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


## **Final Entity Record Format**
Each entity record is structured in the following format before being sent to Kafka:

```json
{
  "eventHeader": { ... },
  "keys": [ "account_id" ],
  "payload": { "account_details", "party_relations", "addresses" }
}
```

### Initial Setup & Arguments

##### Validates command-line arguments. Requires two parameters: job_run_env (environment) and load_date.

```bash
if len(sys.argv) < 3:
    print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
    sys.exit(-1)
```

### Job Initialization

##### Sets the job environment (e.g., LOCAL, QA, PROD), load date, and generates a unique job ID using uuid.

```bash
job_run_env = sys.argv[1].upper()
load_date = sys.argv[2]
job_run_id = "SBDL-" + str(uuid.uuid4())
```

### Configuration

##### Loads environment-specific configurations (e.g., Hive settings, Kafka credentials) via ConfigLoader.

```bash
conf = ConfigLoader.get_config(job_run_env)
enable_hive = True if conf["enable.hive"] == "true" else False
hive_db = conf["hive.database"]
```

### Spark Session & Logging

##### Sets the job environment (e.g., LOCAL, QA, PROD), load date, and generates a unique job ID using uuid.

```bash
spark = Utils.get_spark_session(job_run_env)
logger = Log4j(spark)
```

### Data Loading

##### Loads data from sources (e.g., Hive tables, files) into DataFrames using DataLoader. Behavior depends on job_run_env and Hive configuration.

```bash
accounts_df = DataLoader.read_accounts(...)
parties_df = DataLoader.read_parties(...)
address_df = DataLoader.read_address(...)
```

### Data Transformations

##### Applies transformations to raw data:
- ** get_contract: Extracts contract details from accounts.
- ** get_relations: Processes party relationships.
- ** get_address: Structures address data.

```bash
contract_df = Transformations.get_contract(accounts_df)
relations_df = Transformations.get_relations(parties_df)
relation_address_df = Transformations.get_address(address_df)
```

###  Data Joining

##### Joins transformed DataFrames:
- ** Combines party relations with addresses.
- ** Merges the result with contract data.

```bash
party_address_df = Transformations.join_party_address(relations_df, relation_address_df)
data_df = Transformations.join_contract_party(contract_df, party_address_df)
```

### Final Data Preparation

##### Adds metadata headers to the DataFrame. Prepares Kafka payload by:
- ** Selecting the contractIdentifier as the Kafka key.
- ** Converting the entire row to a JSON string for the value.

```bash
final_df = Transformations.apply_header(spark, data_df)
kafka_kv_df = final_df.select(col("payload.contractIdentifier.newValue").alias("key"), to_json(struct("*")).alias("value"))
```




