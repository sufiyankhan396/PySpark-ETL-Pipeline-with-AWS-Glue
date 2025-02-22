from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

# Initialize Spark
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()

# Read data from S3
df = spark.read.csv("s3://your-bucket/sample_data.csv", header=True, inferSchema=True)

# Transform: Convert names to uppercase
df_transformed = df.withColumn("name", upper(col("name")))

# Write to Snowflake
df_transformed.write.format("snowflake").option("url", "<SNOWFLAKE_URL>").option("dbtable", "employees").save()

print("ETL Pipeline Completed!")
