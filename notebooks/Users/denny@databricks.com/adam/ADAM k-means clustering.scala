// Databricks notebook source exported at Wed, 20 Apr 2016 18:51:42 UTC
// MAGIC %md ## ADAM Genomics Analysis: K-means clustering
// MAGIC This notebook shows how to perform analysis against genomics datasets using the [Big Data Genomics](http://bdgenomics.org) ADAM Project ([0.19.0 Release](http://bdgenomics.org/blog/2016/02/25/adam-0-dot-19-dot-0-release/)).  We perform k-means clustering to predict which region the genome sequence is from and show the confusion matrix.
// MAGIC 
// MAGIC **Attribution:**
// MAGIC * Andy Petrella's [Lightning Fast Genomics with Spark and ADAM](http://www.slideshare.net/noootsab/lightning-fast-genomics-with-spark-adam-and-scala) and [GitHub repository](https://github.com/andypetrella).
// MAGIC * [Big Data Genomics ADAM](http://bdgenomics.org/projects/adam/)
// MAGIC * [ADAM: Genomics Formats and Processing Patterns for Cloud Scale Computing (Berkeley AMPLab)](https://amplab.cs.berkeley.edu/publication/adam-genomics-formats-and-processing-patterns-for-cloud-scale-computing/)

// COMMAND ----------

// MAGIC %md ### Preparation
// MAGIC Below are various tasks to prepare the ADAM datasets.  
// MAGIC * Import various Big Data Genomics libraries
// MAGIC * Set file locations 
// MAGIC * Load Sample [Variant Call Format (VCF) Format file](http://www.1000genomes.org/wiki/Analysis/variant-call-format) and save the file in ADAM format

// COMMAND ----------

// MAGIC %md **WARNING:** 
// MAGIC * When launching the cluster, ensure that the following configurations have been set:
// MAGIC  * `spark.serializer org.apache.spark.serializer.KryoSerializer`
// MAGIC  * `spark.kryo.registrator org.bdgenomics.adam.serialization.ADAMKryoRegistrator`
// MAGIC * The library `org.bdgenomics.utils.misc.Hadoop (utils-misc_2.10-0.2.4.jar)` must be installed /attached to the cluster *prior* to 
// MAGIC * Installing / attaching the `org.bdgenomics.adam.core (adam-core_2.10-0.19.0)` library. 
// MAGIC * These need to be the Scala 2.10 and the cluster needs to be Spark 1.6.1 (Hadoop 2).

// COMMAND ----------

// Import the various Big Data Genomics libraries 
import org.bdgenomics.adam.converters
import org.bdgenomics.formats.avro
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation._
import org.bdgenomics.adam.rdd.ADAMContext

// COMMAND ----------

// Set file locations
val l_vcf = "/databricks-datasets/samples/adam/samples/6-sample.vcf"
val l_tmp = "/tmp/adam/6-sample.adam"
val l_panel = "/databricks-datasets/samples/adam/panel/integrated_call_samples_v3.20130502.ALL.panel"

// COMMAND ----------

// Remove temporary folder
dbutils.fs.rm(l_tmp, true)

// COMMAND ----------

// MAGIC %md #### Quick View of the sample VCF file

// COMMAND ----------

// Take a quick view of the sample VCF file
//dbutils.fs.head(l_vcf)

// COMMAND ----------

// Quick view of the sample VCF file.
dbutils.fs.head(l_vcf)

// COMMAND ----------

// MAGIC %md #### Load Sample VCF file and save into ADAM Parquet format

// COMMAND ----------

// Load Sample VCF file 
val gts:RDD[Genotype] =  sc.loadGenotypes(l_vcf)

// Save Sample VCF file into ADAM format
gts.adamParquetSave(l_tmp)

// COMMAND ----------

// Review the files in the l_tmp folder
display(dbutils.fs.ls(l_tmp))

// COMMAND ----------

// MAGIC %md Here is the first row reformatted JSON
// MAGIC ```
// MAGIC {
// MAGIC 	"variant": {
// MAGIC 		"variantErrorProbability": 100,
// MAGIC 		"contig": {
// MAGIC 			"contigName": "6",
// MAGIC 			"contigLength": null,
// MAGIC 			"contigMD5": null,
// MAGIC 			"referenceURL": null,
// MAGIC 			"assembly": null,
// MAGIC 			"species": null,
// MAGIC 			"referenceIndex": null
// MAGIC 		},
// MAGIC 		"start": 800444,
// MAGIC 		"end": 1239636,
// MAGIC 		"referenceAllele": "G",
// MAGIC 		"alternateAllele": "<CN0>",
// MAGIC 		"svAllele": null,
// MAGIC 		"isSomatic": false
// MAGIC 	},
// MAGIC 	"variantCallingAnnotations": {
// MAGIC 		"variantIsPassing": true,
// MAGIC 		"variantFilters": [
// MAGIC 
// MAGIC 		],
// MAGIC 		"downsampled": null,
// MAGIC 		"baseQRankSum": null,
// MAGIC 		"fisherStrandBiasPValue": null,
// MAGIC 		"rmsMapQ": null,
// MAGIC 		"mapq0Reads": null,
// MAGIC 		"mqRankSum": null,
// MAGIC 		"readPositionRankSum": null,
// MAGIC 		"genotypePriors": [
// MAGIC 
// MAGIC 		],
// MAGIC 		"genotypePosteriors": [
// MAGIC 
// MAGIC 		],
// MAGIC 		"vqslod": null,
// MAGIC 		"culprit": null,
// MAGIC 		"attributes": {
// MAGIC 		}
// MAGIC 	},
// MAGIC 	"sampleId": "HG00096",
// MAGIC 	"sampleDescription": null,
// MAGIC 	"processingDescription": null,
// MAGIC 	"alleles": [
// MAGIC 		"Ref",
// MAGIC 		"Ref"
// MAGIC 	],
// MAGIC 	"expectedAlleleDosage": null,
// MAGIC 	"referenceReadDepth": null,
// MAGIC 	"alternateReadDepth": null,
// MAGIC 	"readDepth": null,
// MAGIC 	"minReadDepth": null,
// MAGIC 	"genotypeQuality": null,
// MAGIC 	"genotypeLikelihoods": [
// MAGIC 
// MAGIC 	],
// MAGIC 	"nonReferenceLikelihoods": [
// MAGIC 
// MAGIC 	],
// MAGIC 	"strandBiasComponents": [
// MAGIC 
// MAGIC 	],
// MAGIC 	"splitFromMultiAllelic": false,
// MAGIC 	"isPhased": true,
// MAGIC 	"phaseSetId": null,
// MAGIC 	"phaseQuality": null
// MAGIC }
// MAGIC ```

// COMMAND ----------

// MAGIC %md ### What happens when you read the ADAM Parquet files directly?

// COMMAND ----------

// What happens when we read the parquet files directly?
//val tmp = sqlContext.read.parquet(l_tmp)
//tmp.registerTempTable("temp")

// COMMAND ----------

// MAGIC %sql -- select * from temp limit 20;

// COMMAND ----------

// MAGIC %md ### Load ADAM Parquet Files
// MAGIC Now that you have saved the VCF file into `l_tmp`, you can process your genetic locus ()

// COMMAND ----------

// Load ADAM Parquet Files
val gts:RDD[Genotype] = sc.loadParquetGenotypes(l_tmp)
gts.cache()

// COMMAND ----------

gts.count()

// COMMAND ----------

// MAGIC %md ### Review the Panel file
// MAGIC This sample panel file obtained from `ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/integrated_call_samples_v3.20130502.ALL.panel` contains the sample, population code (pop), super population code (super_pop), and gender.  Note, the population code definitions can be found at http://www.1000genomes.org/cell-lines-and-dna-coriell.
// MAGIC ![](http://www.1000genomes.org/sites/1000genomes.org/files/documents/1000-genomes-map_11-6-12-2_750.jpg)

// COMMAND ----------

// Load panel file
val panel = sqlContext.read
  .format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", "\\t")
  .load(l_panel)

// Filter for British from England and Scotland (GBR), African Ancestry in Southwest US (ASW), and Han Chinese in Bejing, China (CHB) 
val filteredpanel = panel.select("sample", "pop").where("pop IN ('GBR', 'ASW', 'CHB')")
filteredpanel.registerTempTable("filteredpanel")

// COMMAND ----------

// A quick peak into the filtered panel
display(filteredpanel)

// COMMAND ----------

// Create the filtered panel as a Map[String, String]
val fpanel = filteredpanel
  .rdd
  .map{x => x(0).toString -> x(1).toString}
  .collectAsMap()

// COMMAND ----------

// Broadcast panel
val bPanel = sc.broadcast(fpanel)

// COMMAND ----------

// MAGIC %md ### Filter ADAM Parquet (finalGts) based on the panel

// COMMAND ----------

// Filter out only gts within the panel
val finalGts = gts.filter(g => bPanel.value.contains(g.getSampleId))

// COMMAND ----------

// Get the sample count
val sampleCount = finalGts.map(_.getSampleId.toString.hashCode).distinct.count

// COMMAND ----------

// MAGIC %md ### Obtain Complete GTS only (remove missing variants) 

// COMMAND ----------

import scala.collection.JavaConverters._
import org.bdgenomics.formats.avro._
def variantId(g:Genotype):String = {
  val name = g.getVariant.getContig.getContigName
    val start = g.getVariant.getStart
    val end = g.getVariant.getEnd
    s"$name:$start:$end"
}
def asDouble(g:Genotype):Double = g.getAlleles.asScala.count(_ != GenotypeAllele.Ref)

// COMMAND ----------

// A variant should have sampleCount genotypes
val variantsById = finalGts.keyBy(g => variantId(g).hashCode).groupByKey.cache
val missingVariantsRDD = variantsById.filter { case (k, it) => it.size != sampleCount }.keys

// COMMAND ----------

// Create the set of missing variants
val missingVariants = missingVariantsRDD.collect().toSet

// COMMAND ----------

// Create the complete Gts by filtering out the missingVariants
val completeGts = finalGts.filter { g => ! (missingVariants contains variantId(g).hashCode) }

// COMMAND ----------

// Count
completeGts.count()

// COMMAND ----------

// MAGIC %md ### Create Features Vector and DataFrame
// MAGIC Create the Features Vector to run k-means clustering and DataFrame for easier querying of the data.

// COMMAND ----------

import org.apache.spark.mllib.linalg.{Vector=>MLVector, Vectors}
val sampleToData: RDD[(String, (Double, Int))] = completeGts.map { g => (g.getSampleId.toString, (asDouble(g), variantId(g).hashCode)) }
val groupedSampleToData = sampleToData.groupByKey

def makeSortedVector(gts: Iterable[(Double, Int)]): MLVector = Vectors.dense( gts.toArray.sortBy(_._2).map(_._1) )
val dataPerSampleId:RDD[(String, MLVector)] =
    groupedSampleToData.mapValues { it =>
        makeSortedVector(it)
    }

// COMMAND ----------

// Pull out the features vector
val features:RDD[MLVector] = dataPerSampleId.values

// Create dataPerSampleIdDF
val dataPerSampleIdDF = dataPerSampleId.toDF().withColumnRenamed("_1", "gtc").withColumnRenamed("_2", "features")
dataPerSampleIdDF.registerTempTable("dataPerSampleIdDF")

// Create data DF
val data = sqlContext.sql("select b.pop as label, a.features from dataPerSampleIdDF a join filteredpanel b on b.sample = a.gtc")
data.registerTempTable("data")

// COMMAND ----------

// MAGIC %md ### Execute k-means clustering model
// MAGIC Build k-means clustering model to predict `pop` based on the genomic sequence.

// COMMAND ----------

import org.apache.spark.mllib.clustering.{KMeans,KMeansModel}
val numClusters = 3
val numIterations = 20
// features was initalized above
val clusters:KMeansModel = KMeans.train(features, numClusters, numIterations)
val WSSSE = clusters.computeCost(features)

println(s"Compute Cost: ${WSSSE}")
clusters.clusterCenters.zipWithIndex.foreach { case (center, idx) =>
  println(s"Cluster Center ${idx}: ${center}")
}

// COMMAND ----------

display(clusters, data)

// COMMAND ----------

// MAGIC %md ### Predict and compute confusion matrix

// COMMAND ----------

val predictions: RDD[(String, (Int, String))] = dataPerSampleId.map(elt => {
    (elt._1, ( clusters.predict(elt._2), bPanel.value.getOrElse(elt._1, ""))) 
})

// COMMAND ----------

val confusionMatrix = predictions.collect.toMap.values
    .groupBy(_._2).mapValues(_.map(_._1))
    .mapValues(xs => (1 to 3).map( i => xs.count(_ == i-1)).toList)

// COMMAND ----------

// MAGIC %md #### Show the confusion matrix

// COMMAND ----------

val predictionValues: RDD[(String, Int)] = dataPerSampleId.map(elt => {
    (elt._1, clusters.predict(elt._2)) 
})
predictionValues.toDF().registerTempTable("predictionValues")

// COMMAND ----------

// MAGIC %sql 
// MAGIC select a.pop, b.`_2` as prediction, count(1) as count
// MAGIC   from filteredpanel a
// MAGIC     join predictionValues b
// MAGIC       on b.`_1` = a.sample
// MAGIC   where a.pop = "$pop"
// MAGIC group by a.pop, b.`_2`

// COMMAND ----------

// MAGIC %sql 
// MAGIC select a.pop, b.`_2` as prediction, count(1) as count
// MAGIC   from filteredpanel a
// MAGIC     join predictionValues b
// MAGIC       on b.`_1` = a.sample
// MAGIC group by a.pop, b.`_2`

// COMMAND ----------

// MAGIC %sql 
// MAGIC select a.pop, b.`_2` as prediction, count(1) as count
// MAGIC   from filteredpanel a
// MAGIC     join predictionValues b
// MAGIC       on b.`_1` = a.sample
// MAGIC group by a.pop, b.`_2`

// COMMAND ----------

