# Databricks notebook source exported at Mon, 28 Mar 2016 15:47:05 UTC
# MAGIC %md # Population vs. Median Home Prices Linear Regression model
# MAGIC ### Linear Regression with Single Variable

# COMMAND ----------

# MAGIC %md ## Bring in the City Population vs. Median Housing Price data

# COMMAND ----------

data = sc.textFile("/mnt/tardis6/data/norm2/data2_norm.csv")
data.count()

# COMMAND ----------

# MAGIC %md ## Import Linear Regression with Stochastic Gradient Descent

# COMMAND ----------

from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from numpy import array


# COMMAND ----------

# MAGIC %md ## Generate Linear Regression model
# MAGIC ### Load and parse the data
# MAGIC #### y = Median Housing Price (1)
# MAGIC #### X = Population (0)

# COMMAND ----------

def parsePoint(line):
    values = [float(x) for x in line.replace(',', ' ').split(' ')]
    return LabeledPoint(values[1], [values[0]])
    #return (values[1], [values[0]])
parseddata = data.map(parsePoint)

# COMMAND ----------

# MAGIC %md # Scatterplot the data

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

x = parseddata.map(lambda p: (p.features[0])).collect()
y = parseddata.map(lambda p: (p.label)).collect()

from pandas import *
from ggplot import *
pydf = DataFrame({'x':x,'y':y})
p = ggplot(pydf, aes('x','y')) + \
    geom_point(color='blue') 
display(p)

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

x = parseddata.map(lambda p: (p.features[0])).collect()
y = parseddata.map(lambda p: (p.label)).collect()

from pandas import *
from ggplot import *
pydf = DataFrame({'x':x,'y':y})
p = ggplot(pydf, aes('x','y')) + \
    geom_point(color='blue') 
display(p)

# COMMAND ----------

# MAGIC %md ## Linear Regression with SGD
# MAGIC * Load and parse the data where y = Median Housing Price (values[1]) and x = Population (values[0])
# MAGIC * Building two example models
# MAGIC * Reference pyspark MLLib regression
# MAGIC * * http://spark.apache.org/docs/latest/api/python/pyspark.mllib.html#module-pyspark.mllib.regression

# COMMAND ----------

modelA = LinearRegressionWithSGD.train(parseddata, iterations=100, step=0.01, intercept=True)
modelB = LinearRegressionWithSGD.train(parseddata, iterations=1500, step=0.1, intercept=True)

# COMMAND ----------

print ">>>> ModelA intercept: %r, weights: %r" % (modelA.intercept, modelA.weights)

# COMMAND ----------

print ">>>> ModelB intercept: %r, weights: %r" % (modelB.intercept, modelB.weights)

# COMMAND ----------

# MAGIC %md ## Evaluate the Model
# MAGIC #### Predicted vs. Actual

# COMMAND ----------

valuesAndPredsA = parseddata.map(lambda p: (p.label, modelA.predict(p.features)))
MSE = valuesAndPredsA.map(lambda (v, p): (v - p)**2).mean()
print("ModelA - Mean Squared Error = " + str(MSE))

# COMMAND ----------

valuesAndPredsB = parseddata.map(lambda p: (p.label, modelB.predict(p.features)))
MSE = valuesAndPredsB.map(lambda (v, p): (v - p)**2).mean()
print("ModelB - Mean Squared Error = " + str(MSE))

# COMMAND ----------

# MAGIC %md # Linear Regression Plots

# COMMAND ----------

import numpy as np
from pandas import *
from ggplot import *

x = parseddata.map(lambda p: (p.features[0])).collect()
y = parseddata.map(lambda p: (p.label)).collect()
yA = parseddata.map(lambda p: (modelA.predict(p.features))).collect()
yB = parseddata.map(lambda p: (modelB.predict(p.features))).collect()

pydf = DataFrame({'x':x,'y':y,'yA':yA, 'yB':yB})


# COMMAND ----------

# MAGIC %md ## View the Python Pandas DataFrame (pydf)

# COMMAND ----------

pydf

# COMMAND ----------

# MAGIC %md ## ggplot figure
# MAGIC Now that the Python Pandas DataFrame (pydf), use ggplot and display the scatterplot and the two regression models

# COMMAND ----------

p = ggplot(pydf, aes('x','y')) + \
    geom_point(color='blue') + \
    geom_line(pydf, aes('x','yA'), color='red') + \
    geom_line(pydf, aes('x','yB'), color='green')
display(p)

# COMMAND ----------

p = ggplot(pydf, aes('x','y')) + \
    geom_point(color='blue') + \
    geom_line(pydf, aes('x','yA'), color='red') + \
    geom_line(pydf, aes('x','yB'), color='green')
display(p)

# COMMAND ----------

