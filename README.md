# AWS EMR E-Commerce ETL Pipeline

## Project Overview

This project demonstrates an end-to-end AWS data engineering pipeline using Amazon S3, Amazon EMR, PySpark, AWS Glue Data Catalog, and Amazon Athena.

The pipeline processes e-commerce customer, product, and order data stored in Amazon S3. Amazon EMR with PySpark performs data cleaning, validation, deduplication, joins, transformations, and aggregations. The processed data is stored in Amazon S3 in Parquet format and partitioned for efficient querying.

Amazon Athena is then used to query and validate the curated data through SQL.


### Data Flow

```text
CSV Source Files
        │
        ▼
Amazon S3
Raw Data Layer
        │
        ▼
Amazon EMR
PySpark ETL
        │
        ├── Data Validation
        ├── Duplicate Removal
        ├── Data Cleaning
        ├── Customer + Order + Product Joins
        ├── Business Transformations
        └── Aggregations
        │
        ▼
Amazon S3
Curated Parquet Layer
        │
        ▼
AWS Glue Data Catalog
        │
        ▼
Amazon Athena
SQL Validation & Analytics
```

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

Raw Layer
raw/
├── customers/
│   └── customers.csv
├── orders/
│   └── orders.csv
└── products/
    └── products.csv
Script Layer
scripts/
└── emr_pyspark_etl.py
Curated Layer
curated/
├── ecommerce_sales/
├── category_sales/
└── customer_sales/
EMR PySpark ETL

Amazon EMR is used to run the PySpark ETL job.

The PySpark job performs the following operations:

Read CSV files from Amazon S3
Infer source schemas
Validate required business keys
Remove invalid records
Remove duplicate records
Convert date columns
Join orders with customers
Join orders with products
Filter valid business orders
Add business columns
Calculate revenue
Create customer classifications
Generate category-level aggregations
Generate customer-level aggregations
Write the results as Parquet files
Partition the detailed sales data by year and month
Data Quality

The pipeline performs basic data-quality checks before transformation.

Null validation

Required keys are checked for NULL values:

customers = customers.filter(
    col("customer_id").isNotNull()
)

Orders are validated using:

order_id
customer_id
product_id

Products are validated using:

product_id
Duplicate removal

Duplicates are removed using business keys:

customers = customers.dropDuplicates(["customer_id"])

orders = orders.dropDuplicates(["order_id"])

products = products.dropDuplicates(["product_id"])
Business Transformations

The pipeline joins:

Orders
   +
Customers
   +
Products

The resulting dataset contains customer, product, and order information.

Additional columns are generated:

order_year
order_month
revenue
customer_type
Revenue

Revenue is calculated from the order-level amount:

revenue = order_amount
Customer Type

Customer segments are classified as:

Premium  → High Value
Standard → Regular
Basic    → Basic
Curated Data

The detailed sales data is stored in Parquet format.

The detailed dataset is partitioned using:

order_year
order_month

Example:

ecommerce_sales/
├── order_year=2025/
│   ├── order_month=2/
│   │   └── part-....parquet
│   └── order_month=3/
│       └── part-....parquet
└── ...

Partitioning helps reduce the amount of data scanned when queries filter by year or month.

Aggregated Datasets
Category Sales

Location:

s3://emr-ecommerce-2026/curated/category_sales/

Contains:

category
total_revenue
total_orders
average_order_value
Customer Sales

Location:

s3://emr-ecommerce-2026/curated/customer_sales/

Contains:

customer_id
first_name
last_name
city
total_revenue
total_orders
E-Commerce Sales

Location:

s3://emr-ecommerce-2026/curated/ecommerce_sales/

Contains the detailed transformed sales data.

Athena and Glue Data Catalog

AWS Glue Data Catalog is used to store metadata for the curated Parquet datasets.

Database:

emr_ecommerce_db

Tables:

category_sales
customer_sales
ecommerce_sales

The ecommerce_sales table uses:

order_year
order_month

as partitions.

Athena can then query the curated Parquet data directly from S3.

Athena Validation Queries
1. Total Records
SELECT
    COUNT(*) AS total_records
FROM emr_ecommerce_db.ecommerce_sales;
2. Total Revenue
SELECT
    SUM(revenue) AS total_revenue
FROM emr_ecommerce_db.ecommerce_sales;
3. Revenue by Category
SELECT
    category,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS total_orders
FROM emr_ecommerce_db.ecommerce_sales
GROUP BY category
ORDER BY total_revenue DESC;
4. Revenue by Customer
SELECT
    customer_id,
    first_name,
    last_name,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS total_orders
FROM emr_ecommerce_db.ecommerce_sales
GROUP BY
    customer_id,
    first_name,
    last_name
ORDER BY total_revenue DESC;
IAM and Security

The project uses IAM roles to control access to AWS resources.

EMR Service Role
EMR-Ecommerce-Service-Role

Used by Amazon EMR to manage the EMR cluster and required AWS resources.

EMR EC2 Instance Role
EMR-Ecommerce-EC2-Role

The role is designed following the principle of least privilege and provides access to the project S3 bucket.

Required S3 permissions include:

s3:ListBucket
s3:GetObject
s3:PutObject
s3:DeleteObject

The intended bucket is:

s3://emr-ecommerce-2026/
EMR Cluster Configuration

The project uses Amazon EMR with Apache Spark.

Example learning configuration:

EMR Release: EMR 7.x
Application: Spark
Primary Node: 1
Core Node: 1
Task Nodes: 0

The cluster is used for PySpark processing and is terminated after completing the workload to control costs.

SSH Access

The EMR Primary node can be accessed through SSH for development and troubleshooting.

The project uses an EC2 key pair:

emrproject1

SSH access uses:

Protocol: TCP
Port: 22
Source: My IP

This restricts SSH access to the user's current public IP instead of opening SSH to the entire internet.

Example SSH command:

ssh -i "emrproject1.pem" hadoop@<EMR-PRIMARY-PUBLIC-DNS>
Running the PySpark Job

After connecting to the EMR Primary node:

spark-submit s3://emr-ecommerce-2026/scripts/emr_pyspark_etl.py

The PySpark job reads the raw datasets from S3 and writes the transformed Parquet data to the curated S3 layer.

Project Folder Structure
emr-ecommerce-ETL-Pipeline/
│
├── Data/
│   ├── customers.csv
│   ├── orders.csv
│   └── products.csv
│
├── Diagrams and screenshot/
│   └── aws-emr-ecommerce-architecture.png
│
├── EMR Script/
│   └── emr_pyspark_etl.py
│
├── SQL Queries for validation/
│   └── athena_validation.sql
│
└── README.md
Key Data Engineering Concepts Demonstrated
Amazon S3 data lake
Raw and curated data layers
Amazon EMR
Apache Spark
PySpark
ETL processing
Data validation
Duplicate removal
Data cleansing
Multiple-table joins
Business transformations
Aggregations
Parquet
Partitioning
AWS Glue Data Catalog
Amazon Athena
SQL validation
IAM
EC2 security groups
SSH
Least-privilege access
Cost-aware EMR cluster design
Project Outcome

This project demonstrates an end-to-end AWS data engineering workflow where raw e-commerce data is ingested into Amazon S3, transformed using PySpark on Amazon EMR, stored as optimized Parquet datasets in the S3 curated layer, cataloged using AWS Glue Data Catalog, and queried using Amazon Athena.

The project provides practical experience with distributed data processing, cloud data-lake architecture, ETL development, data quality, partitioning, SQL analytics, IAM security, and EMR cluster management.
