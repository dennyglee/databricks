// Databricks notebook source exported at Mon, 28 Mar 2016 15:41:20 UTC
// MAGIC %md # Salesforce Leads with Machine Learning, Spark SQL, and UDFs
// MAGIC * **Purpose:** To predict **Lead Source** (where a marketing lead came from) based on the **state**
// MAGIC * Example of a **decision tree** classification ML model

// COMMAND ----------

// MAGIC %md ### A View of Salesforce Leads
// MAGIC * Within this Scala notebook, we can run SQL statements by using the `%sql` within the notebook cell
// MAGIC * Below is a view of Salesforce leads by company, state, status, and the lead source (where did the marketing lead come from such as paper ad, digital ad, etc.)

// COMMAND ----------

// MAGIC %sql select state, count(1) as leads from newleadssfdc group by state

// COMMAND ----------

// MAGIC %sql select leadsource, count(1) as leads from newleadssfdc group by leadsource

// COMMAND ----------

// MAGIC %md ### Data Preparation
// MAGIC * Setup the MLlib Decision Tree models
// MAGIC * Read the **state** and **lead source** data from the table

// COMMAND ----------

// Setup MLlib Decision trees
import org.apache.spark.sql._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.DecisionTree
import sqlContext.implicits._

// Obtaining the distinct set of state and lead sources from the table and caching it
val inputDataRaw = sql("""select state, leadsource from newleadssfdc where state != "" and leadsource != """"").cache()

// Collect the distinct lead sources
val distinctLeadSources = inputDataRaw.map(row => row(1)).distinct().collect()

// COMMAND ----------

// MAGIC %md ### Categorize Lead sources
// MAGIC * Run Label reduction to assign lead sources to Paper, Digital, Network Referral

// COMMAND ----------

// Categorize different lead sources
val inputData = inputDataRaw.map{ row =>
  row(1) match {
    case "New Registration Promotion" => Row(row(0),"Digital")
    case "Partner Referral" => Row(row(0),"Network Referral")    
    case "Catalog Mailer" => Row(row(0),"Paper")    
    case "Yellow Pages" => Row(row(0),"Paper") 
    case "PPC-MSN printer ink" => Row(row(0),"Paper") 
    case "Summer End Promotions" => Row(row(0),"Digital")  
    case "Impressivo 1500 Special" => Row(row(0),"Digital") 
    case "Keyword-Yahoo Marketing / Overture-Offic" => Row(row(0),"Digital") 
    case "PPC-Google office furniture" => Row(row(0),"Paper")     
    case "October Newsletter" => Row(row(0),"Paper") 
    case "Distribution Monthly Mailer" => Row(row(0),"Paper") 
    case "Electronics Retail Trade Show" => Row(row(0),"Network Referral")
    case "Ad" => Row(row(0),"Paper") 
    case "Web" => Row(row(0),"Digital") 
    case "August Special Offer" => Row(row(0),"Paper") 
    case "Purchased List" => Row(row(0),"Network Referral") 
    case "Industry Trade Show" => Row(row(0),"Network Referral") 
    case _ => Row(row(0),"Unknown")
  }
}



// COMMAND ----------

// MAGIC %md #### View Lead Sources Categories
// MAGIC * Convert the inputData RDD to a DataFrame (output: inputDataTable) to quickly view the data

// COMMAND ----------

// Import Spark SQL data types
import org.apache.spark.sql.types.{StructType,StructField,StringType};

// define the schema (encoded in a string)
val schemaString = "State LeadSourceCategory"

// Generate the schema based on the string of schema
val schema =
  StructType(
    schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

// Apply the schema to the RDD.
val inputDataFrame = sqlContext.createDataFrame(inputData, schema)

// Register the DataFrames as a table.
inputDataFrame.registerTempTable("inputDataTable")

// COMMAND ----------

// MAGIC %sql SELECT * FROM inputDataTable

// COMMAND ----------

// MAGIC %md ### Build a value-to-index map for each unique categorical value:
// MAGIC * **Feature**: State
// MAGIC * **Label**: Lead Source Category

// COMMAND ----------

// Obtain distinct States and indexes
val distinctStates = inputData.map(row => row(0)).distinct()
val distinctStatesWithIdx = distinctStates.zipWithIndex().collect().toMap

// Obtain distinct Lead Sources and indexes
val distinctLeadSources = inputData.map(row => row(1)).distinct()
val distinctLeadSourcesWithIdx = distinctLeadSources.zipWithIndex().collect().toMap

// Invert the Lead Sources
val invertedLeadSources = distinctLeadSourcesWithIdx.map(_.swap)

// Set the label points
val labeledPoints = inputData.map{ row => 
  val label = distinctLeadSourcesWithIdx(row(1)).toDouble
  val featureVector = Vectors.dense(Array(distinctStatesWithIdx(row(0)).toDouble))
  LabeledPoint(label, featureVector)
}

// COMMAND ----------

// MAGIC %md ### Split the data into **training** and **test** data
// MAGIC * When building the model, it is important to have a separate training and test set of data
// MAGIC * For the purpose of this initial analysis - and because we have non-cleansed data - we will randomly set 90% for training and 10% test

// COMMAND ----------

// 90% training and 10% test dataset
val labeledPointsSplit = labeledPoints.randomSplit(Array(0.90,0.10))
val (trainingLabeledPoints, knownTestLabeledPoints) = 
  (labeledPointsSplit(0), labeledPointsSplit(1))


// COMMAND ----------

// MAGIC %md ### Model Prepartion
// MAGIC * Setup the model **hyperparameters**
// MAGIC *  Define categorical features (states)

// COMMAND ----------

// Setup the model **hyperparameters**
val numStates = distinctStatesWithIdx.size
val numLeadSources = distinctLeadSourcesWithIdx.size

// Iteration-stop criteria 
val maxDepth = 30

// Tuning parameters
val maxBins = numStates * 5
val impurity = "gini"

// Define categorical features (states)
val categoricalFeaturesMap = Map[Int, Int](0 -> numStates)

// COMMAND ----------

// MAGIC %md ### Train and display the **model**
// MAGIC * Using the Decision Tree model, we are training the classifier

// COMMAND ----------

val model = DecisionTree.trainClassifier(trainingLabeledPoints, numLeadSources,
  categoricalFeaturesMap, impurity, maxDepth, maxBins)
model.toDebugString


// COMMAND ----------

// MAGIC %md ### Calculate the **evaluation criteria** against the test data
// MAGIC * Setup the evaluation criteria against the test data
// MAGIC * Determine and print the accuracy (in this case, 65%)

// COMMAND ----------

// Evaluation criteria against the test data
val predictionsAndLabels = trainingLabeledPoints.map{ knownLabeledPoint =>
  (model.predict(knownLabeledPoint.features), knownLabeledPoint.label)
}

// COMMAND ----------

// Determine accuracy
import org.apache.spark.mllib.evaluation.MulticlassMetrics
val modelMetrics = new MulticlassMetrics(predictionsAndLabels)

// Print Accuracy
println(s"Accuracy:  ${(modelMetrics.precision * 100).toInt}% predicted correctly.")

// COMMAND ----------

// MAGIC %md # Test the model
// MAGIC Use **State** to predict **lead source** using our recently created decision tree classification model
// MAGIC * Initial define a prediction function (predictLeadSource)
// MAGIC * The run some sample tests for ("CA", "AL", "IL")

// COMMAND ----------

// Define the prediction function (predictLeadSource)
def predictLeadSource(s: String) = {
  invertedLeadSources(model.predict(Vectors.dense(Array(distinctStatesWithIdx(s).toDouble))).toInt).toString()
}

// COMMAND ----------

// State: California -> Lead Source: Paper
predictLeadSource("CA")

// COMMAND ----------

// State: Alabama -> Lead Source: Digital
predictLeadSource("AL")

// COMMAND ----------

// State: Illinois -> Lead Source: Network Referral
predictLeadSource("IL")

// COMMAND ----------

// MAGIC %md # Join the model back to the data
// MAGIC Create a mapStateLeadSource table that maps the state, actual value, and predicted value
// MAGIC * Create the transformedData RDD that will run through the inputData with the predictLeadSource function for the predicted lead source
// MAGIC * Create a schema, apply it, and create the transformed DataFrame

// COMMAND ----------

// Create new RDD which adds the predicted Lead Source Category (LSC) by State
val transformedData = inputData.map {
  row =>
  Row(row(0), row(1), predictLeadSource(row(0).toString))
}

// Define the schema
val schemaMapString = "State ActualLSC PredictedLSC"

// Generate the schema based on the string of schema
val schemaMap =
  StructType(
    schemaMapString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

// Apply the schema to the RDD.
val transformedDataFrame = sqlContext.createDataFrame(transformedData, schemaMap)

// Register the DataFrames as a table.
transformedDataFrame.registerTempTable("mapStateLeadSource")

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- Top 20 rows mapping state, Actual Lead Source Category, and Predicted Lead Source Category
// MAGIC select * from mapStateLeadSource limit 20;

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- Top 20 rows joining original leads table with the mapping of state, Actual, and Predicted
// MAGIC select f.company, f.state, f.leadsource, m.ActualLSC, m.PredictedLSC, f.status 
// MAGIC   from newleadssfdc f 
// MAGIC     inner join mapStateLeadSource m 
// MAGIC       on m.state = f.state 
// MAGIC  limit 20;

// COMMAND ----------

// MAGIC %md # Comparing Actual vs. Predicted
// MAGIC Below are two maps denoting the Actual Category of Digital vs. Predicted Category of Digital
// MAGIC * Actual vs. Predicted with Paper is pretty close
// MAGIC * But Actual vs. Predicted with Digital is very inaccurate

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- Cache tables for performance
// MAGIC cache table newleadssfdc;
// MAGIC cache table mapStateLeadSource;

// COMMAND ----------

// MAGIC %md #### Comparing Actual vs. Predicted for LSC = Paper

// COMMAND ----------

// MAGIC %sql
// MAGIC -- ---------------------------------------
// MAGIC -- Actual Lead Source Category = 'Paper'
// MAGIC -- ---------------------------------------
// MAGIC select f.state, count(1) as leads 
// MAGIC   from newleadssfdc f 
// MAGIC     inner join mapStateLeadSource m 
// MAGIC        on m.state = f.state 
// MAGIC  where f.state NOT IN ('', 'Burlington', 'VA 24018') 
// MAGIC    and f.leadsource != ''
// MAGIC    and m.ActualLSC = 'Paper' 
// MAGIC  group by f.state

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- ---------------------------------------
// MAGIC -- Predicted Lead Source Category = 'Paper'
// MAGIC -- ---------------------------------------
// MAGIC select f.state, count(1) as leads 
// MAGIC   from newleadssfdc f 
// MAGIC  inner join mapStateLeadSource m 
// MAGIC     on m.state = f.state 
// MAGIC  where f.state NOT IN ('', 'Burlington', 'VA 24018') 
// MAGIC    and f.leadsource != ''
// MAGIC    and m.PredictedLSC = 'Paper' 
// MAGIC  group by f.state

// COMMAND ----------

// MAGIC %md #### Comparing Actual vs. Predicted for LSC = Digital

// COMMAND ----------

// MAGIC %sql
// MAGIC -- ---------------------------------------
// MAGIC -- Actual Lead Source Category = 'Digital'
// MAGIC -- ---------------------------------------
// MAGIC select f.state, count(1) as leads 
// MAGIC   from newleadssfdc f 
// MAGIC     inner join mapStateLeadSource m 
// MAGIC        on m.state = f.state 
// MAGIC  where f.state NOT IN ('', 'Burlington', 'VA 24018') 
// MAGIC    and f.leadsource != ''
// MAGIC    and m.ActualLSC = 'Digital' 
// MAGIC  group by f.state

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- ---------------------------------------
// MAGIC -- Predicted Lead Source Category = 'Digital'
// MAGIC -- ---------------------------------------
// MAGIC select f.state, count(1) as leads 
// MAGIC   from newleadssfdc f 
// MAGIC  inner join mapStateLeadSource m 
// MAGIC     on m.state = f.state 
// MAGIC  where f.state NOT IN ('', 'Burlington', 'VA 24018') 
// MAGIC    and f.leadsource != ''
// MAGIC    and m.PredictedLSC = 'Digital' 
// MAGIC  group by f.state

// COMMAND ----------

val a=1