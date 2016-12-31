## How Apache Spark performs a fast count using the parquet metadata
### Parquet Count Metadata Explanation

**Attribution:** Thanks to Cheng Lian and Nong Li for helping me to understand how this process works.

Below is the basics surrounding how an Apache Spark row `count` uses the Parquet file metadata to determine the count (instead of scanning the entire file).  

For a query like `spark.read.parquet(...).count()`, the Parquet columns are not accessed, instead the requested Parquet schema that is passed down to the `VectorizedParquetRecordReader` is simply an empty Parquet message. 
The count is computed using metadata stored in Parquet file footers.

### Jobs
The jobs and stages behind the `spark.read.parquet(...).count()` can be seen in the Spark DAG (from the Spark UI) below.
<img src="https://github.com/dennyglee/databricks/blob/master/images/1-parquet-count.png" height="250px"/>

Basically, to perform the `count` against this parquet file, there are two jobs created - the first job is to read the file from the data source as noted in the diagram below.

<img src="https://github.com/dennyglee/databricks/blob/master/images/2-Job-0.png" height="400px"/>

The second job has two stages to perform the `count`.

<img src="https://github.com/dennyglee/databricks/blob/master/images/3-Job-1.png" height="400px"/>


### Stages
Diving deeper into the stages, you will notice the following:

**Job 1: Stage 1**
The purpose of this first stage is to dive read the Parquet file as noted by the `FileScanRDD`; there is a subsequent `WholeStageCodeGen` that we will dive into shortly.

<img src="https://github.com/dennyglee/databricks/blob/master/images/4-Job-0-Stage-1.png" height="400px"/>

**Job 1: Stage 2**
The purpose of this second stage is to perform the shuffles as noted by the `ShuffledRowRDD`; there is a subsequent `WholeStageCodeGen` that we will dive into shortly.

<img src="https://github.com/dennyglee/databricks/blob/master/images/5-Job-0-Stage-2.png" height="400px"/>

### InternalRow

Internally, the entire logic surrounding this 
* Not reading any Parquet columns to calculate the count
* Passing of the Parquet schema to the VectorizedParquetRecordReader is actually an empty Parquet message
* Computing the count using the metadata stored in the Parquet file footers.

involves the wrapping of the above within an iterator that returns an `InternalRow` per [InternalRow.scala](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/InternalRow.scala).

To work with the Parquet File format, internally, Apache Spark wraps the logic with an iterator that returns an `InternalRow`; more information can be found in [InternalRow.scala](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/InternalRow.scala).  Ultimately, the `count()` aggregate function interacts with the underlying Parquet data source using this iterator. This is true for both vectorized and non-vectorized Parquet reader.


### Bridging the Dataset.count() with Parquet metadata

To bridge the `Dataset.count()` with the Parquet reader, the path is:
* The `Dataset.count()` call is planned into an aggregate operator with a single count() aggregate function.
* Java code is generated (i.e. `WholeStageCodeGen`) at planning time for the aggregate operator as well as the count() aggregate function.
* The generated Java code interacts with the underlying data source [ParquetFileFormat](https://github.com/apache/spark/blob/2f7461f31331cfc37f6cfa3586b7bbefb3af5547/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L18) with an [RecordReaderIterator](https://github.com/apache/spark/blob/b03b4adf6d8f4c6d92575c0947540cb474bf7de1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/RecordReaderIterator.scala), which is used internally by the Spark data source API.

&nbsp;

### Unpacking all of this
Let's unpack this with links to the code on how this all works; to do this, we'll go backwards on the above flow.

&nbsp;

#### 1. Generated Java Code interacts with the underlying data source

As noted above:

   *The generated Java code interacts with the underlying data source [ParquetFileFormat](https://github.com/apache/spark/blob/2f7461f31331cfc37f6cfa3586b7bbefb3af5547/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L18) with an [RecordReaderIterator](https://github.com/apache/spark/blob/b03b4adf6d8f4c6d92575c0947540cb474bf7de1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/RecordReaderIterator.scala), which is used internally by the Spark data source API.*
   
When reviewing the **Job 0: Stage 0** (as noted above / diagram below), this first job is to read the file from the data source.

<img src="https://github.com/dennyglee/databricks/blob/master/images/2-Job-0.png" height="400px"/>

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
The first part of the `spark.read.parquet(...).count()` query is `spark.read.parquet(...)`.  As noted in **Job 0: Stage 0**, its job is access the `ParquetFileFormat` data source when utilizing the `DataFrameReader`.  This is all within the context of the *Data Source API* `org.apache.spark.sql.execution.datasources`.

##### How does the Java code interact with the underlying data source?

One is through the `RecordReaderIterator` that returns an `InternalRow` as noted above (and via the code flow below):
```
|- package org.apache.spark.sql.execution.datasources.parquet [[ParquetFileFormat.scala#L18]]
	https://github.com/apache/spark/blob/2f7461f31331cfc37f6cfa3586b7bbefb3af5547/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L18

		|- class ParquetFileFormat [[ParquetFileFormat.scala#L51]]
			https://github.com/apache/spark/blob/2f7461f31331cfc37f6cfa3586b7bbefb3af5547/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L51

			|- val iter = new RecordReaderIterator(parquetReader) [[ParquetFileFormat.scala#L399]]
				https://github.com/apache/spark/blob/2f7461f31331cfc37f6cfa3586b7bbefb3af5547/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L399

				|- class RecordReaderIterator[T] [[RecordReaderIterator.scala#L32]]
					https://github.com/apache/spark/blob/b03b4adf6d8f4c6d92575c0947540cb474bf7de1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/RecordReaderIterator.scala#L32

						|- import org.apache.hadoop.mapreduce.RecordReader [[RecordReaderIterator.scala#L22]]
							https://github.com/apache/spark/blob/b03b4adf6d8f4c6d92575c0947540cb474bf7de1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/RecordReaderIterator.scala#L22

						|- import org.apache.spark.sql.catalyst.InternalRow [[RecordReaderIterator.scala#L24]]
							https://github.com/apache/spark/blob/b03b4adf6d8f4c6d92575c0947540cb474bf7de1/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/RecordReaderIterator.scala#L24
```

The other interaction path is via the `VectorizedParquetRecordReader` (and NonVectorized) path; below is the code flow for the former.

```
|- package org.apache.spark.sql.execution.datasources.parquet [[ParquetFileFormat.scala#L18]]
  https://github.com/apache/spark/blob/2f7461f31331cfc37f6cfa3586b7bbefb3af5547/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L18

    |- class ParquetFileFormat [[ParquetFileFormat.scala#L51]]
      https://github.com/apache/spark/blob/2f7461f31331cfc37f6cfa3586b7bbefb3af5547/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L51

        |- val vectorizedReader = new VectorizedParquetRecordReader()
          https://github.com/apache/spark/blob/2f7461f31331cfc37f6cfa3586b7bbefb3af5547/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetFileFormat.scala#L376

            |- VectorizedParquetRecordReader.java#48
              https://github.com/apache/spark/blob/39e2bad6a866d27c3ca594d15e574a1da3ee84cc/sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/VectorizedParquetRecordReader.java#L48

                  |- SpecificParquetRecordReaderBase.java#L151
                    https://github.com/apache/spark/blob/v2.0.2/sql/core/src/main/java/org/apache/spark/sql/execution/datasources/parquet/SpecificParquetRecordReaderBase.java#L151

                        |- totalRowCount
```
Diving into the details a bit, the `SpecificParquetRecordReaderBase.java` references the [Improve Parquet scan performance when using flat schemas commit](https://github.com/apache/spark/commit/cfdd8a1a304d66f2a424800ccc026874e6c5f1a8) as part of [[SPARK-11787] Speed up parquet reader for flat schemas](https://issues.apache.org/jira/browse/SPARK-11787). Note, this commit was included as part of the Spark 1.6 branch.

Ultimately, if the query is a row count, Spark will reading the Parquet metadata to determine the count. If the predicates are fully satisfied by the min/max values, that should work as well though that is not fully verified. 

&nbsp;

#### 2. Generated Java Code interacts with the underlying data source

As noted above:
   *Java code is generated (i.e. `WholeStageCodeGen`) at planning time for the aggregate operator as well as the count() aggregate function.*

Digging into this further, note the actions of **Job 1: Stage 1**

<img src="https://github.com/dennyglee/databricks/blob/master/images/4-Job-0-Stage-1.png" height="400px"/>

It performs a `FileScanRDD` and subsequently the Java byte code is generated via `WholeStageCodeGen`.  Review the code flow, you can see that the same DataSource ApI `org.apache.spark.sql.execution.datasources` in the previous bullet also contains the `FileScanRDD`.

```
|- DataSource.apply(L147)
https://github.com/apache/spark/blob/689de920056ae20fe203c2b6faf5b1462e8ea73c/sql/core/src/main/scala/org/apache/spark/sql/DataFrameReader.scala#L147

	|- package org.apache.spark.sql.execution.datasources	
	https://github.com/apache/spark/blob/689de920056ae20fe203c2b6faf5b1462e8ea73c/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala

		|- FileScanRDD 			
		https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileScanRDD.scala
```

Note, the generation of Java byte code via `WholeStageCodeGen` is reference to the **Code Generation** step per the diagram of Spark's Catalyst Optimizer as part of the Spark SQL Engine. 

![](https://github.com/dennyglee/databricks/blob/master/images/Catalyst-Optimizer.png)

For more information on this section, a great overview is [Structuring Spark: DataFrames, Datasets, and Streaming by Michael Armbrust](http://www.slideshare.net/SparkSummit/structuring-spark-dataframes-datasets-and-streaming-by-michael-armbrust).  If you want to dive even deeper, check out the blog [Deep Dive into Spark SQL’s Catalyst Optimizer](https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html) and [Apache Spark as a Compiler: Joining a Billion Rows per Second on a Laptop](https://databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html).

&nbsp;

#### 3. The Dataset.count() call is planned into an aggregate operator with a single count() aggregate function.
Putting this all back together, the `Dataset.count()` is planned into an aggregate operator as per **Job 1** as noted below.

<img src="https://github.com/dennyglee/databricks/blob/master/images/3-Job-1.png" height="400px"/>

Following `DataFrameReader` > `DataSource.apply` as per above, the same class also connects back to the `Dataset.ofRows` per the code flow below.

```
org.apache.spark.sql.DataFrameReader.load(DataFrameReader.scala:145)
	https://github.com/apache/spark/blob/689de920056ae20fe203c2b6faf5b1462e8ea73c/sql/core/src/main/scala/org/apache/spark/sql/DataFrameReader.scala#L145

	|- sparkSession.baseRelationToDataFrame (L146)
		https://github.com/apache/spark/blob/689de920056ae20fe203c2b6faf5b1462e8ea73c/sql/core/src/main/scala/org/apache/spark/sql/DataFrameReader.scala#L146

		|- Dataset.ofRows(self, LogicalRelation(baseRelation))
			https://github.com/apache/spark/blob/689de920056ae20fe203c2b6faf5b1462e8ea73c/sql/core/src/main/scala/org/apache/spark/sql/SparkSession.scala#L386
```



