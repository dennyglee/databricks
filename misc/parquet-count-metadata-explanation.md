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
The purpose of this first stage is to dive read the Parquet file as noted by the `FileScanRDD`.  There is a subsequent `WholeStageCodeGen` that we will dive into shortly.

<img src="https://github.com/dennyglee/databricks/blob/master/images/4-Job-0-Stage-1.png" height="300px"/>

**Job 1: Stage 2**
The purpose of this second stage is to perform the shuffles as noted by the `ShuffledRowRDD`.  There is a subsequent `WholeStageCodeGen` that we will dive into shortly.

<img src="https://github.com/dennyglee/databricks/blob/master/images/5-Job-0-Stage-2.png" height="300px"/>

### InternalRow

Internally, the entire logic surrounding this 
* Not reading any Parquet columns to calculate the count
* Passing of the Parquet schema to the VectorizedParquetRecordReader is actually an empty Parquet message
* Computing the count using the metadata stored in the Parquet file footers.

involves the wrapping of the above within an iterator that returns an `InternalRow` per [InternalRow.scala](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/InternalRow.scala).

To work with the Parquet File format, internally, Apache Spark wraps the logic with an iterator that returns an `InternalRow`; more information can be found in [InternalRow.scala](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/InternalRow.scala)



The count() aggregate function interacts with the underlying Parquet data source using this iterator. This is true for both vectorized and non-vectorized Parquet reader.




