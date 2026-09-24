# AWS EMR E-Commerce ETL Pipeline

## Project Overview

This project demonstrates an end-to-end AWS data engineering pipeline using Amazon S3, Amazon EMR, PySpark, AWS Glue Data Catalog, and Amazon Athena.

The pipeline processes e-commerce customer, product, and order data stored in Amazon S3. Amazon EMR with PySpark performs data cleaning, validation, deduplication, joins, transformations, and aggregations. The processed data is stored in Amazon S3 in Parquet format and partitioned for efficient querying.

Amazon Athena is then used to query and validate the curated data through SQL.

## Architecture

![AWS EMR E-Commerce Architecture](Diagrams%20and%20screenshot/aws-emr-ecommerce-architecture.png)

### Data Flow

CSV Files
↓
Amazon S3 - Raw Layer
↓
Amazon EMR + PySpark
↓
Data Cleaning and Transformation
↓
Amazon S3 - Curated Layer
↓
AWS Glue Data Catalog
↓
Amazon Athena
↓
SQL Validation and Analytics

---

## AWS Services Used

| Service | Purpose |
|---|---|
| Amazon S3 | Raw and curated data storage |
| Amazon EMR | Distributed data processing |
| Apache Spark | Data processing engine |
| PySpark | ETL and transformation logic |
| AWS Glue Data Catalog | Store table metadata |
| Amazon Athena | SQL querying and validation |
| IAM | Access control and security |
| EC2 Key Pair | SSH access to EMR Primary node |
| Security Groups | Network access control |

---

## Source Data

The project uses three main CSV datasets:

### customers.csv

Contains customer information.

Columns:

- customer_id
- first_name
- last_name
- email
- city
- state
- country
- customer_segment
- registration_date

### orders.csv

Contains order information.

Columns:

- order_id
- customer_id
- product_id
- order_date
- quantity
- order_amount
- order_status

### products.csv

Contains product information.

Columns:

- product_id
- product_name
- category
- subcategory
- unit_price
- cost_price

---

# S3 Data Lake Structure

The project uses the following S3 bucket:

```text
s3://emr-ecommerce-2026/
