import os
from pathlib import Path
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum, avg, round
from dotenv import load_dotenv

# Load environment variables
dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

# Get environment variables
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

# Setup Spark connection
spark_host = "spark://dibimbing-dataeng-spark-master:7077"

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
    pyspark
    .SparkConf()
    .setAppName('OlistAnalysis')
    .setMaster(spark_host)
    .set("spark.jars", "/spark-scripts/jars/postgresql-42.2.20.jar")
))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# Setup PostgreSQL connection
jdbc_url = f'jdbc:postgresql://{postgres_host}:5432/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# Read data from PostgreSQL
orders_df = spark.read.jdbc(
    jdbc_url,
    'public.olist_orders',
    properties=jdbc_properties
)

customers_df = spark.read.jdbc(
    jdbc_url,
    'public.olist_customers',
    properties=jdbc_properties
)

order_items_df = spark.read.jdbc(
    jdbc_url,
    'public.olist_order_items',
    properties=jdbc_properties
)

# Perform analysis
# Join tables
sales_analysis = (
    orders_df
    .join(customers_df, "customer_id")
    .join(order_items_df, "order_id")
)

# Calculate metrics per region
region_performance = (
    sales_analysis
    .groupBy("customer_state")
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("price"), 2).alias("total_revenue"),
        round(avg("price"), 2).alias("avg_order_value"),
        count(col("customer_id").cast("string")).alias("total_customers")  # Fixed this line
    )
)

# Save results to PostgreSQL
region_performance.write \
    .mode("overwrite") \
    .jdbc(
        jdbc_url,
        'public.olist_sales_performance',
        properties=jdbc_properties
    )

# Show results
print("Sales Performance by Region:")
region_performance.show()