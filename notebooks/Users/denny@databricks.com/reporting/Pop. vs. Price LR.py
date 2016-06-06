# Databricks notebook source exported at Mon, 6 Jun 2016 00:57:35 UTC
# MAGIC %md # Population vs. Median Home Prices
# MAGIC #### *Linear Regression with Single Variable*

# COMMAND ----------

# MAGIC %md *Note, this notebook requires Spark 1.6+*

# COMMAND ----------

# MAGIC %scala if (org.apache.spark.BuildInfo.sparkBranch < "1.6") sys.error("Attach this notebook to a cluster running Spark 1.6+")

# COMMAND ----------

# MAGIC %md ### Load and parse the data

# COMMAND ----------

# Use the Spark CSV datasource with options specifying:
#  - First line of file is a header
#  - Automatically infer the schema of the data
data = sqlContext.read.format("com.databricks.spark.csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/databricks-datasets/samples/population-vs-price/data_geo.csv")
data.cache()  # Cache data for faster reuse
data.count()

# COMMAND ----------

display(data)

# COMMAND ----------

data = data.dropna()  # drop rows with missing values
data.count()

# COMMAND ----------

# This will let us access the table from our SQL notebook!
data.registerTempTable("data_geo")

# COMMAND ----------

# MAGIC %md ## Limit data to Population vs. Price
# MAGIC (for our ML example)
# MAGIC 
# MAGIC We also use `LabeledPoint` to convert the feature (population) to a Vector type, to prep the data for ML algorithms.

# COMMAND ----------

from pyspark.mllib.regression import LabeledPoint  # convenience for specifying schema
data = data.select("2014 Population estimate", "2015 median sales price")\
  .map(lambda r: LabeledPoint(r[1], [r[0]]))\
  .toDF()
display(data)

# COMMAND ----------

# MAGIC %md ## Scatterplot of the data using ggplot

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

x = data.map(lambda p: (p.features[0])).collect()
y = data.map(lambda p: (p.label)).collect()

from pandas import *
from ggplot import *
pydf = DataFrame({'pop':x,'price':y})
p = ggplot(pydf, aes('pop','price')) + \
    geom_point(color='blue') 
display(p)

# COMMAND ----------

# MAGIC %md ## Linear Regression
# MAGIC 
# MAGIC **Goal**
# MAGIC * Predict y = 2015 Median Housing Price
# MAGIC * Using feature x = 2014 Population Estimate
# MAGIC 
# MAGIC **References**
# MAGIC * [MLlib LinearRegression user guide](http://spark.apache.org/docs/latest/ml-classification-regression.html#linear-regression)
# MAGIC * [PySpark LinearRegression API](http://spark.apache.org/docs/latest/api/python/pyspark.ml.html#pyspark.ml.regression.LinearRegression)

# COMMAND ----------

# Import LinearRegression class
from pyspark.ml.regression import LinearRegression

# COMMAND ----------

# Define LinearRegression algorithm
lr = LinearRegression()

# COMMAND ----------

# Fit 2 models, using different regularization parameters
modelA = lr.fit(data, {lr.regParam:0.0})
modelB = lr.fit(data, {lr.regParam:100.0})

# COMMAND ----------

print ">>>> ModelA intercept: %r, coefficient: %r" % (modelA.intercept, modelA.coefficients[0])

# COMMAND ----------

print ">>>> ModelB intercept: %r, coefficient: %r" % (modelB.intercept, modelB.coefficients[0])

# COMMAND ----------

# MAGIC %md ## Make predictions
# MAGIC 
# MAGIC Calling `transform()` on data adds a new column of predictions.

# COMMAND ----------

# Make predictions
predictionsA = modelA.transform(data)
display(predictionsA)

# COMMAND ----------

# MAGIC %md ## Evaluate the Model
# MAGIC #### Predicted vs. True label

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(metricName="rmse")
RMSE = evaluator.evaluate(predictionsA)
print("ModelA: Root Mean Squared Error = " + str(RMSE))

# COMMAND ----------

predictionsB = modelB.transform(data)
RMSE = evaluator.evaluate(predictionsB)
print("ModelB: Root Mean Squared Error = " + str(RMSE))

# COMMAND ----------

# MAGIC %md # Linear Regression Plots

# COMMAND ----------

import numpy as np
from pandas import *
from ggplot import *

pop = data.map(lambda p: (p.features[0])).collect()
price = data.map(lambda p: (p.label)).collect()
predA = predictionsA.select("prediction").map(lambda r: r[0]).collect()
predB = predictionsB.select("prediction").map(lambda r: r[0]).collect()

pydf = DataFrame({'pop':pop,'price':price,'predA':predA, 'predB':predB})

# COMMAND ----------

# MAGIC %md ## View the Python Pandas DataFrame (pydf)

# COMMAND ----------

pydf

# COMMAND ----------

# MAGIC %md ## ggplot figure
# MAGIC Now that the Python Pandas DataFrame (pydf), use ggplot and display the scatterplot and the two regression models

# COMMAND ----------

p = ggplot(pydf, aes('pop','price')) + \
    geom_point(color='blue') + \
    geom_line(pydf, aes('pop','predA'), color='red') + \
    geom_line(pydf, aes('pop','predB'), color='green') + \
    scale_x_log10() + scale_y_log10()
display(p)