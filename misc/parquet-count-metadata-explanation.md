## Parquet Count Metadata Explanation

**Attribution:** Thanks to Cheng Lian and Nong Li for helping me to understand how this process works.

Below is the basics surrounding how an Apache Spark row `count` uses the Parquet file metadata to detemrine the count (instead of scanning the entire file).  

For a query like `spark.read.parquet(...).count()`, the Parquet columns are not accessed, instead the requested Parquet schema that is passed down to the `VectorizedParquetRecordReader` is simply an empty Parquet message. 
The count is computed using metadata stored in Parquet file footers.

### Jobs and Stages
The jobs and stages behind the `spark.read.parquet(...).count()` can be seen in the diagram below.
<img src="https://github.com/dennyglee/databricks/blob/master/images/1-parquet-count.png" height="200px"/>

Basically, to perform the `count` against this parquet file, there are two jobs created - the first job is to read the file from the data source as noted in the diagram below.

<img src="https://github.com/dennyglee/databricks/blob/master/images/2-Job-0.png" height="300px"/>

The second job has two stages to perform the `count`.

<img src="https://github.com/dennyglee/databricks/blob/master/images/3-Job-1.png" height="300px"/>







