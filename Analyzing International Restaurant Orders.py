# Databricks notebook source
# Import required libraries from PySpark
from pyspark.sql.types import StructField, StringType, IntegerType, DateType, StructType
from pyspark.sql.functions import year, month, quarter, desc, count


# COMMAND ----------

# Define schema for the product data
prod_schema = StructType([
    StructField('PROD_ID', StringType(), True),   # Product ID
    StructField('PROD_NAME', StringType(), True), # Product Name
    StructField('PRICE', StringType(), True)      # Price of the product
])

# COMMAND ----------

# Load product data from CSV file into a DataFrame with the specified schema
prod_df = spark.read.csv('/FileStore/tables/product.txt', header=False, schema=prod_schema)


# COMMAND ----------

# Display the first 5 rows of the product DataFrame
prod_df.head(5)

# COMMAND ----------

# Define schema for the order data
orders_schema = StructType([
    StructField("PROD_ID", IntegerType(), True),    # Product ID
    StructField("CUST_ID", StringType(), True),     # Customer ID
    StructField("ORDER_DATE", DateType(), True),    # Order date
    StructField("LOCATION", StringType(), True),    # Order location
    StructField("ORDER_TYPE", StringType(), True) # Source of the order
])

# COMMAND ----------

# Load order data from CSV file into a DataFrame with the specified schema
orders_df = spark.read.csv('/FileStore/tables/order.txt', header=False, schema=orders_schema)


# COMMAND ----------

# Display the first 5 rows of the order DataFrame
orders_df.head(5)

# COMMAND ----------

# DBTITLE 1,#Add YEAR, MONTH, QUARTER
# Add additional columns to the order DataFrame for year, month, and quarter derived from the order date
orders_df = orders_df.withColumn('ORDER_YEAR', year(orders_df.ORDER_DATE))
orders_df = orders_df.withColumn('ORDER_MONTH', month(orders_df.ORDER_DATE))
orders_df = orders_df.withColumn('ORDER_QUARTER', quarter(orders_df.ORDER_DATE))


# COMMAND ----------

# Display the updated order DataFrame with the new columns
display(orders_df)

# COMMAND ----------

# Join the product DataFrame with the order DataFrame on the product ID column
sales_df = orders_df.join(prod_df, "PROD_ID")

# COMMAND ----------

# Calculate total spending by each customer
customer_spend = (
    sales_df.groupBy("CUST_ID")  # Group by customer ID
    .agg({"PRICE": "SUM"})       # Aggregate sum of prices
    .withColumnRenamed("sum(PRICE)", "Total Cost") # Rename the aggregated column
    .orderBy(desc("Total Cost")) # Order by total cost in descending order
)
display(customer_spend)

# COMMAND ----------


# Calculate total spending by product category
spend_category_df = (
    sales_df.groupBy("PROD_NAME") # Group by product name
    .agg({"PRICE": "SUM"})        # Aggregate sum of prices
    .withColumnRenamed("sum(PRICE)", "Total Cost") # Rename the aggregated column
    .orderBy(desc("Total Cost"))  # Order by total cost in descending order
)
display(spend_category_df)

# COMMAND ----------

# Calculate total monthly revenue
monthly_revenue_df = (
    sales_df.groupBy("ORDER_MONTH") # Group by order month
    .agg({"PRICE": "SUM"})          # Aggregate sum of prices
    .withColumnRenamed("sum(PRICE)", "Total Cost") # Rename the aggregated column
    .orderBy(desc("Total Cost"))    # Order by total cost in descending order
)
display(monthly_revenue_df)

# COMMAND ----------

# Calculate yearly revenue
Year_revenue_df = (
    sales_df.groupBy("ORDER_YEAR")  # Group by order year
    .agg({"PRICE": "SUM"})          # Aggregate sum of prices
    .withColumnRenamed("sum(PRICE)", "YEARLY SALES") # Rename the aggregated column
    .orderBy(desc("YEARLY SALES"))  # Order by yearly sales in descending order
)
display(Year_revenue_df)

# COMMAND ----------

# Calculate quarterly sales
quarter_sales_df = (
    sales_df.groupBy("ORDER_QUARTER") # Group by order quarter
    .agg({"PRICE": "SUM"})            # Aggregate sum of prices
    .withColumnRenamed("sum(PRICE)", "QUARTER SALES") # Rename the aggregated column
    .orderBy(desc("QUARTER SALES"))   # Order by quarterly sales in descending order
)
display(quarter_sales_df)

# COMMAND ----------

# Calculate monthly sales across different years
month_revenue_df = (
    sales_df.groupBy("ORDER_YEAR", "ORDER_MONTH") # Group by order year and month
    .agg({"PRICE": "sum"})                        # Aggregate sum of prices
    .withColumnRenamed("sum(PRICE)", "MONTH SALES") # Rename the aggregated column
    .orderBy(desc("MONTH SALES"))                 # Order by monthly sales in descending order
)
display(month_revenue_df)

# COMMAND ----------

# Identify top 5 most frequently sold products
FREQ_PROD = (
    sales_df.groupBy("PROD_NAME")    # Group by product name
    .agg(count(prod_df.PROD_ID).alias("FREQ_PROD")) # Count the frequency of product ID
    .orderBy("FREQ_PROD", ascending=False) # Order by frequency in descending order
    .limit(5)                         # Limit to top 5 products
)
display(FREQ_PROD)

# COMMAND ----------

# Calculate the frequency of purchases by each customer
CUST_SALES = (
    sales_df.groupBy("CUST_ID")  # Group by customer ID
    .agg(count(sales_df.CUST_ID).alias("FREQ_cust")) # Count the frequency of customer ID
    .orderBy("FREQ_cust", ascending=False) # Order by frequency in descending order
)
display(CUST_SALES)

# COMMAND ----------

# Calculate the frequency of purchases by SOURCE OF ORDER
ORDER_TYPE = (
    sales_df.groupBy("ORDER_TYPE")  # Group by customer ID
    .agg(count(sales_df.ORDER_TYPE).alias("FREQ_ORDER_TYPE")) # Count the frequency of customer ID
    .orderBy("FREQ_ORDER_TYPE", ascending=False) # Order by frequency in descending order
)
display(ORDER_TYPE)
