import os
from pathlib import Path
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, col, when, round, avg, count
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

# Read CSV files
orders_df = spark.read.csv(
    "/data/olist/olist_orders_dataset.csv",
    header=True,
    inferSchema=True
)

customers_df = spark.read.csv(
    "/data/olist/olist_customers_dataset.csv",
    header=True,
    inferSchema=True
)

order_items_df = spark.read.csv(
    "/data/olist/olist_order_items_dataset.csv",
    header=True,
    inferSchema=True
)

# Load data to PostgreSQL
orders_df.write \
    .mode("overwrite") \
    .jdbc(
        jdbc_url,
        'public.olist_orders',
        properties=jdbc_properties
    )

customers_df.write \
    .mode("overwrite") \
    .jdbc(
        jdbc_url,
        'public.olist_customers',
        properties=jdbc_properties
    )

order_items_df.write \
    .mode("overwrite") \
    .jdbc(
        jdbc_url,
        'public.olist_order_items',
        properties=jdbc_properties
    )

# Verify data loaded successfully
print("Verifying data loaded successfully...")
for table in ['olist_orders', 'olist_customers', 'olist_order_items']:
    count = spark.read.jdbc(
        jdbc_url,
        f'public.{table}',
        properties=jdbc_properties
    ).count()
    print(f"{table}: {count} rows")