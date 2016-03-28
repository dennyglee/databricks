# Databricks notebook source exported at Mon, 28 Mar 2016 17:05:36 UTC
# MAGIC %md ## Statistical and Mathematical Functions with DataFrames in Spark
# MAGIC Accompaning notebook for the blog post [Statistical and Mathematical Functions with DataFrames in Spark](https://databricks.com/blog/2015/06/02/statistical-and-mathematical-functions-with-dataframes-in-spark.html).
# MAGIC 
# MAGIC We will discuss the following functions:
# MAGIC * Random data generation
# MAGIC * Summary and descriptive statistics
# MAGIC * Sample covariance and correlation
# MAGIC * Cross tabulation (a.k.a. contingency table)
# MAGIC * Frequent items
# MAGIC * Mathematical functions

# COMMAND ----------

# MAGIC %md ### Random Data Generation
# MAGIC Random data generation is useful for testing of existing algorithms and implementing randomized algorithms, such as random projection. We provide methods under sql.functions for generating columns that contains i.i.d. (*independently and identically distributed) values drawn from a distribution, e.g., uniform (`rand`),  and standard normal (`randn`).

# COMMAND ----------

from pyspark.sql.functions import rand, randn
# Create a DataFrame with one int column and 10 rows.
df = sqlContext.range(0, 10)
df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

# Generate two other columns using uniform distribution and normal distribution.
df.select("id", rand(seed=10).alias("uniform"), randn(seed=27).alias("normal")).show()


# COMMAND ----------

display(df.select("id", rand(seed=10).alias("uniform"), randn(seed=27).alias("normal")))

# COMMAND ----------

# MAGIC %md ### Summary and Descriptive Statistics
# MAGIC 
# MAGIC The first operation to perform after importing data is to get some sense of what it looks like. For numerical columns, knowing the descriptive summary statistics can help a lot in understanding the distribution of your data. The function `describe` returns a DataFrame containing information such as number of non-null entries (count), mean, standard deviation, and minimum and maximum value for each numerical column.

# COMMAND ----------

from pyspark.sql.functions import rand, randn
# A slightly different way to generate the two random columns
df = sqlContext.range(0, 10).withColumn('uniform', rand(seed=10)).withColumn('normal', randn(seed=27))
#df.describe().show()
display(df.describe())


# COMMAND ----------

#df.describe('uniform', 'normal').show()
display(df.describe('uniform', 'normal'))

# COMMAND ----------

from pyspark.sql.functions import mean, min, max
#df.select([mean('uniform'), min('uniform'), max('uniform')]).show()
display(df.select([mean('uniform'), min('uniform'), max('uniform')]))

# COMMAND ----------

# MAGIC %md ### Sample covariance and correlation
# MAGIC 
# MAGIC Covariance is a measure of how two variables change with respect to each other. A positive number would mean that there is a tendency that as one variable increases, the other increases as well. A negative number would mean that as one variable increases, the other variable has a tendency to decrease. The sample covariance of two columns of a DataFrame can be calculated as follows:

# COMMAND ----------

from pyspark.sql.functions import rand
df = sqlContext.range(0, 10).withColumn('rand1', rand(seed=10)).withColumn('rand2', rand(seed=27))


# COMMAND ----------

df.stat.cov('rand1', 'rand2')

# COMMAND ----------

df.stat.cov('id', 'id')

# COMMAND ----------

df.stat.corr('rand1', 'rand2')

# COMMAND ----------

df.stat.corr('id', 'id')

# COMMAND ----------

# MAGIC %md ### Cross Tabulation (Contingency Table)
# MAGIC [Cross Tabulation](http://en.wikipedia.org/wiki/Contingency_table) provides a table of the frequency distribution for a set of variables. Cross-tabulation is a powerful tool in statistics that is used to observe the statistical significance (or independence) of variables. 

# COMMAND ----------

# Create a DataFrame with two columns (name, item)
names = ["Alice", "Bob", "Mike"]
items = ["milk", "bread", "butter", "apples", "oranges"]
df = sqlContext.createDataFrame([(names[i % 3], items[i % 5]) for i in range(100)], ["name", "item"])

# Take a look at the first 10 rows.
#df.show(10)
display(df.limit(10))

# COMMAND ----------

#df.stat.crosstab("name", "item").show()
display(df.stat.crosstab("name", "item"))

# COMMAND ----------

# MAGIC %md ### Frequent Items
# MAGIC Figuring out which items are frequent in each column can be very useful to understand a dataset. In Spark 1.4, users will be able to find the frequent items for a set of columns using DataFrames. We have implemented an [one-pass algorithm](http://dx.doi.org/10.1145/762471.762473) proposed by Karp et al. 

# COMMAND ----------

df = sqlContext.createDataFrame([(1, 2, 3) if i % 2 == 0 else (i, 2 * i, i % 4) for i in range(100)], ["a", "b", "c"])
# df.show(10)
display(df.limit(10))

# COMMAND ----------

# MAGIC %md ## Determine frequent items
# MAGIC Using SQL `group by` statements to determine frequent items in the columns

# COMMAND ----------

# Register as a temp table to run SQL statements below
df.registerTempTable("df")

# COMMAND ----------

# MAGIC %sql select a, count(1) as cnt from df group by a order by cnt desc;

# COMMAND ----------

# MAGIC %sql select b, count(1) as cnt from df group by b order by cnt desc;

# COMMAND ----------

# MAGIC %sql select c, count(1) as cnt from df group by c order by cnt desc;

# COMMAND ----------

# MAGIC %md Given the above DataFrame, the following code finds the frequent items that show up 40% of the time for each column:

# COMMAND ----------

freq = df.stat.freqItems(["a", "b", "c"], 0.4)
freq.collect()[0]

# COMMAND ----------

# MAGIC %md Per above `{a = 1}, {b = 2}, {c = 1, 3}` are frequent items, note that `{a = 65}` and `{b = 130}` are false positives.
# MAGIC 
# MAGIC You can also find frequent items for column combinations, by creating a composite column using the struct function:

# COMMAND ----------

from pyspark.sql.functions import struct
freq = df.withColumn('ab', struct('a', 'b')).stat.freqItems(['ab'], 0.4)
freq.collect()[0]

# COMMAND ----------

# MAGIC %md From the above example, the combination of `a=99 and b=198`, and `a=1 and b=2` appear frequently in this dataset. Note that `a=99 and b=198` is a false positive.

# COMMAND ----------

# MAGIC %md ### Mathematical Functions
# MAGIC Spark 1.4 also added a suite of mathematical functions. Users can apply these to their columns with ease. The list of math functions that are supported come from [this file](https://github.com/apache/spark/blob/efe3bfdf496aa6206ace2697e31dd4c0c3c824fb/python/pyspark/sql/functions.py#L109). The inputs need to be columns functions that take a single argument, such as `cos, sin, floor, ceil`. For functions that take two arguments as input, such as `pow, hypot`, either two columns or a combination of a double and column can be supplied.

# COMMAND ----------

from pyspark.sql.functions import *
df = sqlContext.range(0, 10).withColumn('uniform', rand(seed=10) * 3.14)

# you can reference a column or supply the column name
mathfun = df.select('uniform',\
          toDegrees('uniform'),\
          (pow(cos(df['uniform']), 2) + pow(sin(df.uniform), 2)). \
          alias("cos^2 + sin^2"))

# display
display(mathfun)