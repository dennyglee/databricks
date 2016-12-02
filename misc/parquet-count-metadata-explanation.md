## Parquet Count Metadata Explanation

**Attribution:** Thanks to Cheng Lian and Nong Li for helping me to understand how this process works.

Below is the basics surrounding how an Apache Spark row `count` uses the Parquet file metadata to detemrine the count (instead of scanning the entire file).  

For a query like `spark.read.parquet(...).count()`, the Parquet columns are not accessed, instead the requested Parquet schema that is passed down to the `VectorizedParquetRecordReader` is simply an empty Parquet message. 
The count is computed using metadata stored in Parquet file footers.

### Jobs
The jobs and stages behind the `spark.read.parquet(...).count()` can be seen in the diagram below.
<img src="https://github.com/dennyglee/databricks/blob/master/images/1-parquet-count.png" height="200px"/>

Basically, to perform the `count` against this parquet file, there are two jobs created - the first job is to read the file from the data source as noted in the diagram below.

<img src="https://github.com/dennyglee/databricks/blob/master/images/2-Job-0.png" height="300px"/>

The second job has two stages to perform the `count`.

<img src="https://github.com/dennyglee/databricks/blob/master/images/3-Job-1.png" height="300px"/>


### Stages
Diving deeper into the stages, you will notice the following:

**Job 1: Stage 1**
The purpose of this first stage is to dive read the Parquet file as noted by the `FileScanRDD`; there is a subsequent `WholeStageCodeGen` that we will dive into shortly.

<img src="https://github.com/dennyglee/databricks/blob/master/images/4-Job-0-Stage-1.png" height="300px"/>

**Job 1: Stage 2**
The purpose of this second stage is to perform the shuffles as noted by the `ShuffledRowRDD`; there is a subsequent `WholeStageCodeGen` that we will dive into shortly.

<img src="https://github.com/dennyglee/databricks/blob/master/images/5-Job-0-Stage-2.png" height="300px"/>

### InternalRow

Internally, the entire logic surrounding this 
* Not reading any Parquet columns to calculate the count
* Passing of the Parquet schema to the VectorizedParquetRecordReader is actually an empty Parquet message
* Computing the count using the metadata stored in the Parquet file footers.

involves the wrapping of the above within an iterator that returns an `InternalRow` per [InternalRow.scala](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/InternalRow.scala).

To work with the Parquet File format, internally, Apache Spark wraps the logic with an iterator that returns an `InternalRow`; more information can be found in [InternalRow.scala](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/InternalRow.scala).  Ultimate, the `count()` aggregate function interacts with the underlying Parquet data source using this iterator. This is true for both vectorized and non-vectorized Parquet reader.


### Bridging the Dataset.count() with Parquet metadata

To bridge the `Dataset.count()` with the Parquet reader, the path is:
* The `Dataset.count()` call is planned into an aggregate operator with a single count() aggregate function.
* Java code is generated (i.e. `WholeStageCodeGen`) at planning time for the aggregate operator as well as the count() aggregate function.
* The generated Java code interacts with the underlying data source [ParquetFileFormat](https://github.com/apache/spark/blob/2f7461f31331cfc37f6cfa3586b7bbefb3af5547/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L18) with an [RecordReaderIterator](https://github.com/apache/spark/blob/b03b4adf6d8f4c6d92575c0947540cb474bf7de1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/RecordReaderIterator.scala), which is used internally by the Spark data source API.



### Unpacking all of this
Let's unpack this with links to the code on how this all works; to do this, we'll go backwards on the above flow.


#### 1. Generated Java Code interacts with the underlying data source

As noted above:

   *The generated Java code interacts with the underlying data source [ParquetFileFormat](https://github.com/apache/spark/blob/2f7461f31331cfc37f6cfa3586b7bbefb3af5547/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L18) with an [RecordReaderIterator](https://github.com/apache/spark/blob/b03b4adf6d8f4c6d92575c0947540cb474bf7de1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/RecordReaderIterator.scala), which is used internally by the Spark data source API.*
   
When reviewing the **Job 0: Stage 0** (as noted above / diagram below), this first job is to read the file from the data source.

<img src="https://github.com/dennyglee/databricks/blob/master/images/2-Job-0.png" height="300px"/>

The breakdown of the code flow can be seen below:


```
org.apache.spark.sql.DataFrameReader.load (DataFrameReader.scala:145)
   https://github.com/apache/spark/blob/689de920056ae20fe203c2b6faf5b1462e8ea73c/sql/core/src/main/scala/org/apache/spark/sql/DataFrameReader.scala#L145

	|- DataSource.apply(L147)
		https://github.com/apache/spark/blob/689de920056ae20fe203c2b6faf5b1462e8ea73c/sql/core/src/main/scala/org/apache/spark/sql/DataFrameReader.scala#L147

		|- package org.apache.spark.sql.execution.datasources
			https://github.com/apache/spark/blob/689de920056ae20fe203c2b6faf5b1462e8ea73c/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala

			|- package org.apache.spark.sql.execution.datasources.parquet [[ParquetFileFormat.scala#L18]]
				https://github.com/apache/spark/blob/2f7461f31331cfc37f6cfa3586b7bbefb3af5547/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L18

    			|- class ParquetFileFormat [[ParquetFileFormat.scala#L51]]
    				https://github.com/apache/spark/blob/2f7461f31331cfc37f6cfa3586b7bbefb3af5547/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L51
```
The first part of the `spark.read.parquet(...).count()` query is `spark.read.parquet(...)`.  As noted in **Job 0: Stage 0**, its job is access the `ParquetFileFormat` data source when utilizing the `DataFrameReader`.







