# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
flight_file_location = "/FileStore/tables/flightData.csv"
pass_file_location = "/FileStore/tables/passengers.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_flight = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(flight_file_location)


df_pass = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(pass_file_location)



display(df_flight)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rename columns of flight dataset

# COMMAND ----------

df_flight = df_flight.withColumnRenamed("date","journey_date").withColumnRenamed("passengerId","pid").withColumnRenamed("flightid","fid")
df_flight.show(10)

# COMMAND ----------

# Create a view or table

temp_table_name = "flightdata"

df_flight.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# %sql

#Query the created temp table in a SQL cell */

# select * from `flightData`*/

# COMMAND ----------

import pyspark.sql.functions as sqlf
# df_flight.show(10)
df_flight2 = df_flight.withColumn("journey_month",sqlf.lit(sqlf.month('journey_date'))).drop("journey_date")
df_flight2.printSchema()

# COMMAND ----------

# df_flight2.groupBy("fid","journey_month").count().show()
df_flight2.groupBy("journey_month").count()\
    .withColumnRenamed("journey_month","Month").\
        withColumnRenamed("count","Number of Flights")\
            .orderBy("journey_month")\
                .show()

# df_flight2.orderBy("journey_month").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename passenger dataset columns

# COMMAND ----------

# df_pass.printSchema()

df_pass = df_pass.withColumnRenamed("passengerId","pid")\
.withColumnRenamed("firstName","fname")\
.withColumnRenamed("lastName","lname")

# df_pass.count()
df_pass.show(10)

# COMMAND ----------

# df_pass.count() ##15500
# df_pass.select("fname","lname").distinct().count() #15496
# df_pass.orderBy("fname","lname").show()

# df_stage = df_flight2.join(df_pass, df_pass.pid == df_flight2.pid, "inner") \
#      .select("df_flight2.*","df_pass.fname")
# df_stage.show()

df_stage = df_flight2.join(df_pass, df_pass.pid == df_flight2.pid, "inner").select(df_flight2["*"],df_pass["lname"])

df_stage.show()

# COMMAND ----------

df_stage

# COMMAND ----------

import pyspark.sql.functions as fnsql #row_number

df_stage.select("*").groupBy("pid") \
    .agg(fnsql.count("pid").alias("countx"))\
        .filter(fnsql.col("countx") >20).show()



# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "flightData_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)
