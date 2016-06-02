// Databricks notebook source exported at Thu, 2 Jun 2016 05:04:27 UTC
// MAGIC %md ## Utilize spark-sas7bdat to read SAS .sas7bdat files
// MAGIC * Read SAS .sas7bdat files (genotype examples)
// MAGIC * Utilize the [spark-sas7bdat](https://spark-packages.org/package/saurfang/spark-sas7bdat) Spark-package ([documentation](https://github.com/saurfang/spark-sas7bdat))
// MAGIC  * A library for parsing SAS data (sas7bdat) with Spark SQL. This also includes a `SasInputFormat` designed for Hadoop mapreduce. This format is splittable when input is uncompressed thus can achieve high parallelism for a large SAS file.
// MAGIC  * Thanks to the splittable `SasInputFormat`, we are able to convert a 200GB (1.5Bn rows) .sas7bdat file to .csv files using 2000 executors in under 2 minutes.

// COMMAND ----------

// MAGIC %fs ls /mnt/tardis6/genotype/

// COMMAND ----------

val filepath = "/mnt/tardis6/genotype/genotype.sas7bdat"
val df = sqlContext.read.format("com.github.saurfang.sas.spark").load(filepath)

// COMMAND ----------

display(df)

// COMMAND ----------

