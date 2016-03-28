# Databricks notebook source exported at Mon, 28 Mar 2016 15:49:04 UTC
# MAGIC %md ## Spark 1.6 R Improvements
# MAGIC This R notebook contains an example of [SPARK-9836](https://issues.apache.org/jira/browse/SPARK-9836) **R-like statistics for GLMs** - (Partial) R-like stats for ordinary least squares via summary(model)

# COMMAND ----------

# Create the DataFrame
df <- createDataFrame(sqlContext, iris)

# Fit a gaussian GLM model over the dataset.
model <- glm(Sepal_Length ~ Sepal_Width + Species, data = df, family = "gaussian")

# COMMAND ----------

# Model summary are returned in a similar format to R's native glm().
summary(model)

# COMMAND ----------

