# Databricks notebook source
# MAGIC %md
# MAGIC # Survival Analysis with SparkR
# MAGIC 
# MAGIC **Attribution**: The original notebook can be found in the **Databricks Guide** [Survival Analysis with SparkR](https://docs.cloud.databricks.com/docs/latest/databricks_guide/index.html#08%20Language%20References/00%20SparkR/4%20Survival%20Analysis%20Using%20AFT.html).
# MAGIC 
# MAGIC There were two modifications
# MAGIC * Added `display(fitted)` to denote that the `label` is the `futime` (survival or censoring time) 
# MAGIC * Added the use of `spark.ml`'s `AFTSurvivalRegression` 
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC This notebook will give a brief introduction to survival analysis and will provide an application example on the ovarian dataset using SparkR.  
# MAGIC This notebook works on Spark 2.0.
# MAGIC 
# MAGIC Survival analysis studies the expected duration of time until an event happens. This time is often associated with some risk factors or treatment taken on the subject. One main task is to learn the relationship quantitatively and make prediction on future subjects.
# MAGIC 
# MAGIC **Goal**: Estimate survival time distribution and make predictions of survival time based on individual features.
# MAGIC 
# MAGIC The data for survival analysis have their own characteristics compared to those in standard regression analysis, including
# MAGIC - censoring: data could be incomplete for some subject
# MAGIC   - could be the case where no events happen before end of the experiment
# MAGIC   - could lose track in the study
# MAGIC   - could exit the experiment
# MAGIC - non-negative survival time

# COMMAND ----------

# MAGIC %md
# MAGIC We formalize the framework of survival analysis. First are some basic concepts.
# MAGIC - *survival time* \\(T\\): time until an event happens, characterized by its PDF \\(f\\) and CDF \\(F\\).
# MAGIC - *survival function* \\(S(t) = P(T > t) = 1 - F(t)\\), where \\(F\\) is the cumulative distribution function of \\(T\\).
# MAGIC - *hazard function*
# MAGIC $$
# MAGIC h(t) = \lim_{\epsilon \downarrow 0} \frac{P(T \in [t, t+\epsilon] | T \geq t)}{\epsilon} = \frac{f(t)}{S(t)}.
# MAGIC $$
# MAGIC 
# MAGIC Survival data often include
# MAGIC - *observed time* \\(U = min(T, C)\\), where \\(C\\) is the cencoring time
# MAGIC - *censoring indicator* \\(\delta = I(T \leq C)\\), 0 indicates censored, 1 uncensored.
# MAGIC - *independent variables* \\(Z\\): risk factors or treatment

# COMMAND ----------

# MAGIC %md
# MAGIC #### Methods
# MAGIC There are various methods available for survival analysis, such as
# MAGIC - Proportional Hazards Model
# MAGIC - Accelerated Failure Time Model
# MAGIC - Kaplan���Meier Estimator
# MAGIC 
# MAGIC For details, refer to
# MAGIC - [Wikipedia: Survival Analysis](https://en.wikipedia.org/wiki/Survival_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC We use Accelerated Failure Time (AFT) model to describe the survival time \\(T\\). It assumes that the effect of a covariate is to accelerate or decelerate the life course of an event by some constant. Specifically, 
# MAGIC $$
# MAGIC \log(T) = \beta_0 + \beta' Z + \epsilon,
# MAGIC $$
# MAGIC where \\(\beta_0\\) is the intercept, \\(\beta\\) unknown coefficient vector and \\(\epsilon\\) is random noise term. Depending on the distribution of noise, survival time \\(T\\) can follow different distributions. Among those, [Weibull](https://en.wikipedia.org/wiki/Weibull_distribution) and [log-logistic](https://en.wikipedia.org/wiki/Log-logistic_distribution) are the most popular. The current MLlib implementation assumes Weibull distribution.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading and Visualization
# MAGIC 
# MAGIC In this example, we will be using the Ovarian Cancer Survival Data that is shipped with the R package *survival*.

# COMMAND ----------

if (!require("survival")) {
  install.packages("survival")
  library(survival)
}

# COMMAND ----------

# MAGIC %md
# MAGIC First we create a SparkR DataFrame from this dataset.

# COMMAND ----------

df <- createDataFrame(ovarian)

# COMMAND ----------

# MAGIC %md
# MAGIC We inspect the dataset by printing out the schema and displaying a few instances. 

# COMMAND ----------

# MAGIC %md
# MAGIC Here are descriptions of the features for reference.
# MAGIC - **futime**: survival or censoring time
# MAGIC - **fustat**: censoring status
# MAGIC - **age**:	in years
# MAGIC - **resid_ds**:	residual disease present (1=no, 2=yes)
# MAGIC - **rx**:	treatment group
# MAGIC - **ecog.ps**:	ECOG performance status (1 is better, see reference)

# COMMAND ----------

printSchema(df)

# COMMAND ----------

show(df)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Fitting and Summarization

# COMMAND ----------

# MAGIC %md
# MAGIC The main function is `spark.survreg`, which can be used by calling
# MAGIC `spark.survreg(data, formula)`.  
# MAGIC The two arguments are:
# MAGIC   - `data`: a SparkDataFrame for training
# MAGIC   - `formula`: a symbolic description of the model to be fitted. Formula operators including '~', ':', '+', and '-' are supported, but the operator '.' is not currently supported.
# MAGIC   
# MAGIC It returns a fitted AFT survival regression model object, which can be later used for prediction.

# COMMAND ----------

# MAGIC %md
# MAGIC Similar to the syntax in the `survreg` R package, the left hand side of the formula should be defined by `Surv(time, event)`, where
# MAGIC - `time`: follow-up time for right censored data
# MAGIC - `event`: indicator of whether the event has occurred, i.e. censored or not - 0 for censored and 1 for uncensored.
# MAGIC 
# MAGIC The right hand side of the formula is a combination of predictors as in standard regression.

# COMMAND ----------

model <- spark.survreg(df, Surv(futime, fustat) ~ ecog_ps + rx)

# COMMAND ----------

# MAGIC %md
# MAGIC The model is fit by finding the maximum likelihood estimator (MLE) of the parameters.
# MAGIC 
# MAGIC A summary of the fitted model can be printed. 

# COMMAND ----------

modelSummary <- summary(model)
print(modelSummary)

# COMMAND ----------

# MAGIC %md
# MAGIC The summary includes intercept and coefficients for the predictors. Note that `Log(scale)` term is specific for Weibull AFT model, which is the result of the scale parameter in Weibull distribution (or the logarithm of Weibull, Gumbel distribution).

# COMMAND ----------

# MAGIC %md
# MAGIC We can use the fitted model to make predictions. For demonstration purposes, we compute the fitted values on the training data and display the results.

# COMMAND ----------

fitted <- predict(model, df)

# COMMAND ----------

# MAGIC %md
# MAGIC The *prediction* column is appended to the original DataFrame. It matches the prediction results by predict function on survreg object in R, which is the predicted values on the original scale of the data (mean predicted value at scale = 1.0).

# COMMAND ----------

# Display the original DataFrame
display(df)

# COMMAND ----------

# Display the fitted data
display(fitted) 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualization of Survival Curves

# COMMAND ----------

# MAGIC %md
# MAGIC We are going to use the model coefficients and plot the survival curves.

# COMMAND ----------

intercept <- modelSummary$coefficients[1]
coefficients <- modelSummary$coefficients[c(2, 3)]

# COMMAND ----------

# MAGIC %md
# MAGIC First find out the estimated scale parameters in Weibull distribution for each level of the predictors. Note: in this dataset we only have 4 different combinations.

# COMMAND ----------

plotParams <- data.frame(rx = rep(c(1, 2), each = 2), ecog_ps = rep(c(1, 2), times = 2))
scale <- exp(intercept + as.matrix(plotParams) %*% coefficients)
display(cbind(plotParams, scale))

# COMMAND ----------

# MAGIC %md
# MAGIC We find out the probability sequences for each combination of feature values.

# COMMAND ----------

tSeq <- seq(0, 5E3, 50)
probs <- data.frame(t = tSeq)
for (i in 1:4) { 
  probs[, paste("(rx, ecog.ps) = (", toString(plotParams[i, ]), ")", sep = "")] <- pweibull(tSeq, shape = 1, scale = scale[i], lower.tail = F)
}

# COMMAND ----------

# MAGIC %md
# MAGIC Then we plot survival curves corresponding to each of the combinations.

# COMMAND ----------

library(ggplot2)
library(reshape2)

# COMMAND ----------

melted <- melt(probs, id.vars="t", variable.name="group", value.name="prob")
ggplot(data=melted, aes(x=t, y=prob, group=group, color=group)) + geom_line()

# COMMAND ----------

# MAGIC %md
# MAGIC From the curves above, we can also see the evidence of the key assumption of AFT models: different feature values stretch out or contract the actual survival time (compared with [Propotional Harzards model](https://en.wikipedia.org/wiki/Proportional_hazards_model)); that is, the distribution of \\(T(Z)\\) is the same as that of \\(T(Z')\\) multiplied by some constant, where the constant only depends on \\(Z, Z'\\), and not on the time considered.
# MAGIC 
# MAGIC In the plot, one curve can be obtained from one another via stretching out or pulling to the vertical axis by some uniform proprotion across all `prob` values.

# COMMAND ----------

# MAGIC %md ## Using the AFTSurvivalRegression Model
# MAGIC Let's use the `AFTSurvivalRegression` model with the `ovarian` dataset; for more information, please refer to [log-linear models for survival analysis](http://spark.apache.org/docs/latest/ml-classification-regression.html#survival-regression).

# COMMAND ----------

# Create temporary DataFrame `df`
createOrReplaceTempView(df, 'df')

# COMMAND ----------

# MAGIC %scala
# MAGIC // Create new `training` DataFrame va
# MAGIC val df = spark.sql("select * from df") 

# COMMAND ----------

# MAGIC %scala
# MAGIC // Display the data
# MAGIC display(df)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Create the training dataset: label, censor, features
# MAGIC // Create VectorAssembler to convert specific DataFrames columns into Vector 
# MAGIC val assembler = new VectorAssembler()
# MAGIC   .setInputCols(Array("ecog_ps", "rx"))
# MAGIC   .setOutputCol("features")
# MAGIC 
# MAGIC // Transform via VectorAssembler and rename columns
# MAGIC val training = assembler.transform(df).select($"futime".alias("label"), $"fustat".alias("censor"), $"features")
# MAGIC 
# MAGIC // Review data
# MAGIC display(training)

# COMMAND ----------

# MAGIC %scala
# MAGIC // Run the same AFTSurvivalRegression found at: http://spark.apache.org/docs/latest/ml-classification-regression.html#survival-regression
# MAGIC import org.apache.spark.ml.linalg.Vectors
# MAGIC import org.apache.spark.ml.regression.AFTSurvivalRegression
# MAGIC 
# MAGIC val quantileProbabilities = Array(0.3, 0.6)
# MAGIC val aft = new AFTSurvivalRegression()
# MAGIC   .setQuantileProbabilities(quantileProbabilities)
# MAGIC   .setQuantilesCol("quantiles")
# MAGIC 
# MAGIC val model = aft.fit(training)
# MAGIC 
# MAGIC // Print the coefficients, intercept and scale parameter for AFT survival regression
# MAGIC println(s"Coefficients: ${model.coefficients} Intercept: " +
# MAGIC   s"${model.intercept} Scale: ${model.scale}")
# MAGIC model.transform(training).show(false)

# COMMAND ----------

# MAGIC %md ## Discussion
# MAGIC The R `survival` package algorithm `survreg` is a regression for a parametric survival model with the following results:
# MAGIC  
# MAGIC ```
# MAGIC $coefficients
# MAGIC                  Value
# MAGIC (Intercept)  6.8966930
# MAGIC ecog_ps     -0.3850426
# MAGIC rx           0.5286457
# MAGIC Log(scale)  -0.1234418
# MAGIC ```
# MAGIC 
# MAGIC These are the same results as the AFTSurvivalRegression:
# MAGIC 
# MAGIC `coefficients: [-0.38504260226158354,0.5286456699308621] Intercept: 6.896693045148861 Scale: 0.8838730674435205`

# COMMAND ----------


