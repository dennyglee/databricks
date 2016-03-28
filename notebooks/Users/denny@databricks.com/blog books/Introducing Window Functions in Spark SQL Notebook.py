# Databricks notebook source exported at Mon, 28 Mar 2016 17:06:02 UTC
# MAGIC %md ## Introducing Window Functions in Spark SQL Notebook
# MAGIC This notebook provides the code samples on from the blog post [Introducing Window Functions in Spark SQL](https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html).

# COMMAND ----------

# MAGIC %md ### Generate Sample Dataset
# MAGIC Below is a generated sample dataset for us to run our window functions

# COMMAND ----------

data = \
  [("Thin", "Cell Phone", 6000),
  ("Normal", "Tablet", 1500),
  ("Mini", "Tablet", 5500),
  ("Ultra thin", "Cell Phone", 5500),
  ("Very thin", "Cell Phone", 6000),
  ("Big", "Tablet", 2500),
  ("Bendable", "Cell Phone", 3000),
  ("Foldable", "Cell Phone", 3000),
  ("Pro", "Tablet", 4500),
  ("Pro2", "Tablet", 6500)]
df = sqlContext.createDataFrame(data, ["product", "category", "revenue"])
df.registerTempTable("productRevenue")

# COMMAND ----------

# Output the tabular dataset
display(df)

# COMMAND ----------

# MAGIC %md ### Question: What are the best selling and second best selling products in every category?
# MAGIC Below is the SQL query used to answer this question by using window function `dense_rank`

# COMMAND ----------

# MAGIC %sql SELECT
# MAGIC   product,
# MAGIC   category,
# MAGIC   revenue
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     product,
# MAGIC     category,
# MAGIC     revenue,
# MAGIC     dense_rank() OVER (PARTITION BY category ORDER BY revenue DESC) as rank
# MAGIC   FROM productRevenue) tmp
# MAGIC WHERE
# MAGIC   rank <= 2

# COMMAND ----------

# MAGIC %md ### Question: What is the difference between the revenue of a product and the revenue of the best selling product in the same category as this product?

# COMMAND ----------

import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func

# Window function partioned by Category and ordered by Revenue
windowSpec = \
  Window \
    .partitionBy(df['category']) \
    .orderBy(df['revenue'].desc()) \
    .rangeBetween(-sys.maxsize, sys.maxsize)
    
# Create dataframe based on the productRevenue table    
dataFrame = sqlContext.table("productRevenue")

# Calculate the Revenue difference
revenue_difference = \
  (func.max(dataFrame['revenue']).over(windowSpec) - dataFrame['revenue'])
  
# Generate a new dataframe (original dataframe and the revenue difference)
revenue_diff = dataFrame.select(
  dataFrame['product'],
  dataFrame['category'],
  dataFrame['revenue'],
  revenue_difference.alias("revenue_difference"))

# Display revenue_diff
display(revenue_diff)