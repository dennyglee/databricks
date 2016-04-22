# Databricks notebook source exported at Fri, 22 Apr 2016 04:31:46 UTC
# MAGIC %md ## Generalized Linear Models in SparkR
# MAGIC 
# MAGIC The dataset we will operate on is the publicly available [airlines](http://stat-computing.org/dataexpo/2009/the-data.html) dataset, which contains twenty years of flight records (from 1987 to 2008). We are interested in predicting airline arrival delay based on the flight departure delay, aircraft type, and distance traveled.  We will read the data from the CSV format using the [spark-csv](http://spark-packages.org/package/databricks/spark-csv) package and join it with an auxiliary [planes table](http://stat-computing.org/dataexpo/2009/supplemental-data.html) with details on individual aircraft.

# COMMAND ----------

# MAGIC %md ### Datasets
# MAGIC The airlines dataset and planes table are available in the `/databricks-datasets/asa` folder.

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/asa/

# COMMAND ----------

# MAGIC %md ### Preprocessing
# MAGIC We use functionality from the DataFrame API to apply some preprocessing to the input. As part of the preprocessing, we decide to drop rows containing null values by applying dropna() to the DataFrame.

# COMMAND ----------

# Obtain airlines dataset
#airlines <- read.df(sqlContext, path="/databricks-datasets/asa/airlines/2002.csv", source="com.databricks.spark.csv", header="true", inferSchema="true")
airlines <- read.df(sqlContext, path="/databricks-datasets/asa/airlines/", source="com.databricks.spark.csv", header="true", inferSchema="true")

# For Databricks Community Edition, please use this smaller file
#airlines <- read.df(sqlContext, path="/databricks-datasets/asa/small/small.csv", source="com.databricks.spark.csv", header="true", inferSchema="true")

# Obtain planes table
planes <- read.df(sqlContext, "/databricks-datasets/asa/planes/plane-data.csv", source="com.databricks.spark.csv", header="true", inferSchema="true")

# Rename some of the columns in the `planes` table
colnames(planes)[1] <- "tail_number"
colnames(planes)[9] <- "plane_year"

# Join airlines and planes dataset by $TailNum
joined <- join(airlines, planes, airlines$TailNum == planes$tail_number)

# Setup training dataset 
training <- dropna(joined)

# COMMAND ----------

# MAGIC %md A quick view of the data `training` dataset

# COMMAND ----------

display(select(training, "aircraft_type", "Distance", "ArrDelay", "DepDelay"))

# COMMAND ----------

# MAGIC %md ### Training
# MAGIC The next step is to use MLlib by calling [glm()](http://spark.apache.org/docs/latest/api/R/glm.html) with a formula specifying the model variables. We specify the Gaussian family here to indicate that we want to perform linear regression. MLlib caches the input DataFrame and launches a series of Spark jobs to fit our model over the distributed dataset.

# COMMAND ----------

model <- glm(ArrDelay ~ DepDelay + Distance + aircraft_type, family = "gaussian", data = training)


# COMMAND ----------

# MAGIC %md ### Evaluation
# MAGIC As with R’s native models, coefficients can be retrieved using the summary() function.

# COMMAND ----------

summary(model)

# COMMAND ----------

# MAGIC %md Note that the **aircraft_type** feature is categorical. Under the hood, SparkR automatically performs one-hot encoding of such features so that it does not need to be done manually. Beyond String and Double type features, it is also possible to fit over [MLlib Vector](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.linalg.Vector) features, for compatibility with other MLlib components.
# MAGIC 
# MAGIC To evaluate our model we can also use [predict()](http://spark.apache.org/docs/latest/api/R/predict.html) just like in R. We can pass in the training data or another DataFrame that contains test data.

# COMMAND ----------

preds <- predict(model, training)
errors <- select(preds, preds$label, preds$prediction, preds$aircraft_type, alias(preds$label - preds$prediction, "error"))

# COMMAND ----------

display(sample(errors, F, .0001))

# COMMAND ----------

