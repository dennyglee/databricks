# Databricks notebook source
# MAGIC %md # Koalas - Simplifying PySpark with Pandas
# MAGIC 
# MAGIC This notebook provides a sample example of scaling your machine learning using [Koalas: Pandas API on Apache Spark](https://github.com/databricks/koalas).
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/fire-koala.jpg" width=150/>
# MAGIC 
# MAGIC Pandas is the de facto standard (single-node) dataframe implementation in Python, while Apache Spark is the de facto standard for big data processing. We can use Koalas to:
# MAGIC * Use Pandas (in lieu of PySpark) syntax
# MAGIC * Easily scale your machine learning utilizing Apache Spark with Pandas syntax
# MAGIC 
# MAGIC For more information:
# MAGIC * [Koalas Documentation](https://koalas.readthedocs.io/)
# MAGIC * [Koalas GitHub Repo](https://github.com/databricks/koalas)
# MAGIC * [koalas (PyPI)](https://pypi.org/project/koalas/)
# MAGIC * [10 minutes to Koalas](https://koalas.readthedocs.io/en/latest/getting_started/10min.html)
# MAGIC 
# MAGIC Dependencies:
# MAGIC * Please install `koalas` and `yellowbrick` Python libraries

# COMMAND ----------

import pandas as pd
import pyspark
import databricks.koalas as ks

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Data Source
# MAGIC We will start by reading in the [Kaggle Boston Housing dataset](https://www.kaggle.com/c/boston-housing/data).
# MAGIC 
# MAGIC ![](https://storage.googleapis.com/kaggle-competitions/kaggle/5315/logos/front_page.png)
# MAGIC 
# MAGIC **Housing Values in Suburbs of Boston**
# MAGIC The medv variable is the target variable.
# MAGIC 
# MAGIC **Data description**
# MAGIC The Boston data frame has 506 rows and 14 columns.
# MAGIC 
# MAGIC This data frame contains the following columns:
# MAGIC 
# MAGIC | columns | description |
# MAGIC | :------- | :----------- |
# MAGIC | crim |  per capita crime rate by town. |
# MAGIC | zn | proportion of residential land zoned for lots over 25,000 sq.ft. |
# MAGIC | indus | proportion of non-retail business acres per town. |
# MAGIC | chas |  Charles River dummy variable (= 1 if tract bounds river; 0 otherwise). |
# MAGIC | nox |  nitrogen oxides concentration (parts per 10 million). |
# MAGIC | rm | average number of rooms per dwelling. |
# MAGIC | age |  proportion of owner-occupied units built prior to 1940. |
# MAGIC | dis |  weighted mean of distances to five Boston employment centres. |
# MAGIC | rad |  index of accessibility to radial highways. |
# MAGIC | tax | full-value property-tax rate per \$10,000. |
# MAGIC | ptratio | pupil-teacher ratio by town. |
# MAGIC | black | 1000(Bk - 0.63)^2 where Bk is the proportion of blacks by town. |
# MAGIC | lstat | lower status of the population (percent). |
# MAGIC | medv | median value of owner-occupied homes in \$1000s. |
# MAGIC 
# MAGIC Sources:
# MAGIC * Harrison, D. and Rubinfeld, D.L. (1978) Hedonic prices and the demand for clean air. J. Environ. Economics and Management 5, 81–102.
# MAGIC * Belsley D.A., Kuh, E. and Welsch, R.E. (1980) Regression Diagnostics. Identifying Influential Data and Sources of Collinearity. New York: Wiley.
# MAGIC 
# MAGIC Resources:
# MAGIC * A great more indepth blog post is Susan Li's [Building A Linear Regression with PySpark and MLlib](https://towardsdatascience.com/building-a-linear-regression-with-pyspark-and-mllib-d065c3ba246a)

# COMMAND ----------

# MAGIC %md ### Download the Boston Housing dataset

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/dennylee/samples/boston/ && wget -O /dbfs/tmp/dennylee/samples/boston/boston-housing.csv https://raw.githubusercontent.com/databricks/tech-talks/master/datasets/boston-housing.csv && ls -al /dbfs/tmp/dennylee/samples/boston/

# COMMAND ----------

# Configure file path
dbfs_path = '/tmp/dennylee/samples/boston/boston-housing.csv'

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Load Data Using Pandas Syntax via Koalas
# MAGIC We can load our data using the Pandas syntax `.read_csv()` to read the CSV file and use the Pandas syntax `.head()` to review the top 5 rows of our Spark DataFrame.

# COMMAND ----------

# Read CSV to pandas dataframe
pdf = pd.read_csv('/dbfs/tmp/dennylee/samples/boston/boston-housing.csv')

# Display it
pdf.head(10)

# COMMAND ----------

# Convert pandas dataframe to Koalas DataFrame
kdf = ks.DataFrame(pdf)

# Display it
kdf.head(10)

# COMMAND ----------

# MAGIC %md ## Exploratory Data Analysis
# MAGIC We will use a combination of `display` and `koalas` to review our data.

# COMMAND ----------

# MAGIC %md ### Determine Possible Linear Correlation Between Multiple Independent Variables
# MAGIC Let's start off by creating a Scatterplot matrix of the different variables by using Databricks `display()`

# COMMAND ----------

def displayKoalas(df):
  display(df.toPandas())

# COMMAND ----------

# View Scatterplot of data
displayKoalas(kdf)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `rm` and `medv`
# MAGIC In the above scatterplot, you can see a possible correlation between `rm` (average rooms per dwelling) and `medv` (median value of owner-occupied homes).  This can be better seen in the following scatterplot of just these two variables.

# COMMAND ----------

# View Scatterplot of data
displayKoalas(kdf)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `lstat` and `medv`
# MAGIC In the above scatterplot, you can see a possible negative linear correlation between `lstat` (lower status of population) and `medv` (median value of owner-occupied homes).  This can be better seen in the following scatterplot of just these two variables.

# COMMAND ----------

# View Scatterplot of data
displayKoalas(kdf)

# COMMAND ----------

# MAGIC %md #### Use Pandas `.corr` to Calculate Correlation Coefficents 
# MAGIC We can quickly calculate the correlation matrix of all attributes with `medv` using Pandas `.corr`.

# COMMAND ----------

# Calculate using Pandas `corr`
pdf_corr = kdf.toPandas().corr()

# Add Index 
pdf_corr['index1'] = pdf_corr.index

# Display values related to `medv`
display(pdf_corr.loc[:, ['index1', 'medv']])

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Use Koalas for Pandas Syntax
# MAGIC * Drop rows with missing values via `.dropna()`
# MAGIC * Rename columns using `.columns`

# COMMAND ----------

# drop rows with missing values
kdf = kdf.dropna()  
kdf.count()

# COMMAND ----------

# New column names
column_names = ['ID', 'crime', 'zone', 'industry', 'bounds_river', 'nox', 'rooms', 'age', 'distance', 'radial_highway', 'tax', 'pupil_teacher', 'black_proportion', 'lower_status', 'median_value']

# Rename columns
kdf.columns = column_names

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Choosing Features
# MAGIC Reviewing the correlation coefficient matrix and scatterplots, let's choose features that have slighty stronger positive or negative correlation to the `median_value` where `abs(correlation coefficients) >= 0.4`.

# COMMAND ----------

# Limit feature columns to abs(correlation coefficients) >= 0.4 
featureColumns = ['crime', 'industry', 'nox', 'rooms', 'tax', 'pupil_teacher', 'lower_status']

# COMMAND ----------

# MAGIC %md ## Build our Linear Regression Model
# MAGIC 
# MAGIC Let's build our Linear Regression model using Scikit-Learn.

# COMMAND ----------

# Re-generate Pandas DataFrame from updated Koala DataFrame
pdf = kdf.toPandas()

# Split training and test datasets
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(pdf[featureColumns].values, pdf['median_value'].values, test_size=0.20, random_state=567248)

# COMMAND ----------

from sklearn.linear_model import Ridge
from yellowbrick.regressor import ResidualsPlot

# Instantiate the linear model and visualizer
ridge = Ridge()
visualizer = ResidualsPlot(ridge, size=(1000, 800))

visualizer.fit(X_train, y_train)  # Fit the training data to the model
visualizer.score(X_test, y_test)  # Evaluate the model on the test data
visualizer.poof()                 # Draw/show/poof the data

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Discussion
# MAGIC While this is a small dataset example, as you increase the iterations and/or data sizes by using `koalas`.  While there is great functionality with the `pandas` syntax, it can be at times very slow.  But with `koalas`, you can easily scale the same code while using PySpark in the backend.