from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, count, sum, avg, year, month, to_date, round, when )

# Create Spark session
spark = SparkSession.builder \
    .appName("EcommerceDataPipeline") \
    .getOrCreate()

# S3 paths
customers_path = "s3://emr-ecommerce-2026/raw/customers/"
orders_path = "s3://emr-ecommerce-2026/raw/orders/"
products_path = "s3://emr-ecommerce-2026/raw/products/"

sales_output_path = "s3://emr-ecommerce-2026/curated/ecommerce_sales/"
category_output_path = "s3://emr-ecommerce-2026/curated/category_sales/"
customer_output_path = "s3://emr-ecommerce-2026/curated/customer_sales/"

# Read source data
customers = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(customers_path)

orders = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(orders_path)

products = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(products_path)


# Check source data
print("Customers count:", customers.count())
print("Orders count:", orders.count())
print("Products count:", products.count())

customers.printSchema()
orders.printSchema()
products.printSchema()


# Remove invalid records
customers = customers.filter(
    col("customer_id").isNotNull()
)

orders = orders.filter(
    col("order_id").isNotNull()
).filter(
    col("customer_id").isNotNull()
).filter(
    col("product_id").isNotNull()
)

products = products.filter(
    col("product_id").isNotNull()
)


# Remove duplicate records
customers = customers.dropDuplicates(["customer_id"])
orders = orders.dropDuplicates(["order_id"])
products = products.dropDuplicates(["product_id"])


# Convert order date to date type
orders = orders.withColumn(
    "order_date",
    to_date(col("order_date"))
)


# Join orders with customers
sales = orders.join(
    customers,
    on="customer_id",
    how="inner"
)


# Join with products
sales = sales.join(
    products,
    on="product_id",
    how="inner"
)


# Keep only valid business orders
sales = sales.filter(
    col("order_status").isin("Completed", "Shipped")
)


# Select required columns
sales = sales.select(
    "order_id",
    "customer_id",
    "product_id",
    "first_name",
    "last_name",
    "city",
    "state",
    "customer_segment",
    "product_name",
    "category",
    "subcategory",
    "quantity",
    "order_amount",
    "order_status",
    "order_date"
)

# Add business columns
sales = sales.withColumn(
    "order_year",
    year("order_date")
)

sales = sales.withColumn(
    "order_month",
    month("order_date")
)

sales = sales.withColumn(
    "revenue",
    round(col("order_amount"), 2)
)

sales = sales.withColumn(
    "customer_type",
    when(
        col("customer_segment") == "Premium",
        "High Value"
    ).when(
        col("customer_segment") == "Standard",
        "Regular"
    ).otherwise(
        "Basic"
    )
)

# Display transformed data
print("Final sales data:")
sales.show(20, truncate=False)

# Revenue by category
category_sales = sales.groupBy(
    "category"
).agg(
    round(sum("revenue"), 2).alias("total_revenue"),
    count("order_id").alias("total_orders"),
    round(avg("revenue"), 2).alias("average_order_value")
).orderBy(
    col("total_revenue").desc()
)

print("Revenue by category:")
category_sales.show(truncate=False)

# Revenue by customer
customer_sales = sales.groupBy(
    "customer_id",
    "first_name",
    "last_name",
    "city"
).agg(
    round(sum("revenue"), 2).alias("total_revenue"),
    count("order_id").alias("total_orders")
).orderBy(
    col("total_revenue").desc()
)

print("Revenue by customer:")
customer_sales.show(truncate=False)

# Write detailed sales data
sales.write \
    .mode("overwrite") \
    .partitionBy("order_year", "order_month") \
    .parquet(sales_output_path)

# Write category summary
category_sales.write \
    .mode("overwrite") \
    .parquet(category_output_path)

# Write customer summary
customer_sales.write \
    .mode("overwrite") \
    .parquet(customer_output_path)

print("EMR PySpark job completed successfully.")
spark.stop()