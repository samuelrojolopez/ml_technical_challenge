from os.path import abspath
from pyspark.sql import SparkSession

warehouse_location = abspath('spark-warehouse')

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Forex processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file from the HDFS
df = spark.read.csv("/dataset_credit_risk/dataset_credit_risk.csv", header=True)

# Show the read file
df.show(n=10)
