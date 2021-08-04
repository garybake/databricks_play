# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS diamonds;
# MAGIC 
# MAGIC CREATE TABLE diamonds
# MAGIC USING csv
# MAGIC OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from diamonds

# COMMAND ----------

# MAGIC %python
# MAGIC diamonds = spark.read.csv("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header="true", inferSchema="true")
# MAGIC diamonds.write.format("delta").mode("overwrite").save("/delta/diamonds")

# COMMAND ----------

df = spark.read.format("delta").load("/delta/diamonds")

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------


