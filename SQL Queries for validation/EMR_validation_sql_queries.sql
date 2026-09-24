-- 1. Total number of records
SELECT
    COUNT(*) AS total_records
FROM emr_ecommerce_db.ecommerce_sales;


-- 2. Total revenue
SELECT
    SUM(revenue) AS total_revenue
FROM emr_ecommerce_db.ecommerce_sales;


-- 3. Revenue by category
SELECT
    category,
    SUM(revenue) AS total_revenue,
    COUNT(*) AS total_orders
FROM emr_ecommerce_db.ecommerce_sales
GROUP BY category
ORDER BY total_revenue DESC;


-- 4. Revenue by customer
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