# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# at run time, pass in value in processed column later
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration" 

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - read the CSV file using the spark dataframe reader API

# COMMAND ----------

# to see mounts 
display(dbutils.fs.mounts())
# we see it where raw is
# we see file route, copy the circuits one

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1newdl/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# contain schema
# structtype = represent row
# structfield = column and specify data type for each column
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

# put first row in header
# identify schema itself and so the data type . 'infershema' reads df again making it slow
# so rather set our own schema and apply
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

# check type and display to check
type(circuits_df)
display(circuits_df)

# COMMAND ----------

#to see datatype
circuits_df.printSchema()

# COMMAND ----------

# show min max value to determine data type
circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - select required columns

# COMMAND ----------

# for the 4th method, col
from pyspark.sql.functions import col

# COMMAND ----------

# 4 ways
# circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")
# below 3 ways can change column name itself while the first method only calls the column names

# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)
# circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude")  \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - add ingestion date to the dataframe

# COMMAND ----------

# now in includes common_function
# from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# adding column object timestamp 
# to add column object from literal value "Prod" in column "env", use withColumn("env", lit("Prod"))
# circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) 
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1newdl/processed/circuits

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1newdl/processed/circuits"))

# COMMAND ----------

dbutils.notebook.exit("Success")
