# Databricks notebook source exported at Mon, 28 Mar 2016 15:40:24 UTC
# MAGIC %md ## Movie Recommendations with MLlib
# MAGIC 
# MAGIC This notebook is based on the post [Scalable Collaborative Filtering with Spark MLlib](https://databricks.com/blog/2014/07/23/scalable-collaborative-filtering-with-spark-mllib.html) applied to the **MovieLens** dataset.  This is the same dataset and set of instructions used within the Spark Summit 2014 Training [Movie Recommendation with MLlib Hands-on Exercises](https://databricks-training.s3.amazonaws.com/movie-recommendation-with-mllib.html).
# MAGIC 
# MAGIC ####Recommendation algorithm
# MAGIC We are using **collaborative filtering** which recommends items based on what similar users like, e.g. recommend video games after a game console purchase because other users purchased video games with their game console purchase.  Spark MLlib implements the collaborative filtering algorithm **Alternating Least Squares (ALS)**.
# MAGIC 
# MAGIC 
# MAGIC ####Dataset
# MAGIC * The [MovieLens](http://movielens.umn.edu/) dataset which contains 72,000 users on 10,000 movies with 10 million ratings.
# MAGIC * You can find the dataset within the Spark Summit 2014 Training Archive [USB drive (download)](https://s3-us-west-2.amazonaws.com/databricks-meng/usb.zip) within the *$usb/data/moviellens/large* folder.
# MAGIC 
# MAGIC ####References
# MAGIC * [Python Ranking Metrics Example](https://github.com/apache/spark/blob/ed47b1e660b830e2d4fac8d6df93f634b260393c/examples/src/main/python/mllib/ranking_metrics_example.py)
# MAGIC * [Python Recommendation Example](https://github.com/apache/spark/blob/master/examples/src/main/python/mllib/recommendation_example.py)
# MAGIC * [MovieLensALS.scala](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/MovieLensALS.scala) code
# MAGIC * [org.apache.spark.ml.recommendation.ALS](https://spark.apache.org/docs/1.5.1/api/scala/index.html#org.apache.spark.ml.recommendation.ALS)
# MAGIC * [MLlib Evaluation Metrics MD](https://github.com/apache/spark/blob/2ecbe02d5b28ee562d10c1735244b90a08532c9e/docs/mllib-evaluation-metrics.md)

# COMMAND ----------

# MAGIC %md ## TODO:
# MAGIC * Call out https://databricks.com/blog/2015/04/24/recent-performance-improvements-in-apache-spark-sql-python-dataframes-and-more.html
# MAGIC * Convert to DataFrame
# MAGIC  * http://stackoverflow.com/questions/32932271/implicit-recomendation-with-ml-spark-and-data-frames
# MAGIC  * https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/recommendation/ALS.scala
# MAGIC  * http://spark.apache.org/docs/latest/mllib-collaborative-filtering.html#collaborative-filtering

# COMMAND ----------

# MAGIC %md ### Review mounted MovieLens datasets
# MAGIC * View the MovieLens datasets in their folders

# COMMAND ----------

# Setup AWS configuration
import urllib
ACCESS_KEY = "[REPLACE_WITH_ACCESS_KEY]"
SECRET_KEY = "[REPLACE_WITH_SECRET_KEY]"
ENCODED_SECRET_KEY = urllib.quote(SECRET_KEY, "")
AWS_BUCKET_NAME = "[REPLACE_WITH_BUCKET_NAME]"
MOUNT_NAME = "mdl"

# Mount S3 bucket
dbutils.fs.mount("s3n://%s:%s@%s/" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/tardis6/movielens/"))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ###Load Data
# MAGIC * This section of cells loads the MovieLens ratings, movies, and personal Ratings datasets

# COMMAND ----------

from pyspark.mllib.recommendation import ALS, Rating

# Parses a rating record in MovieLens format userId::movieId::rating::timestamp .
def parseRating(line):
    fields = line.strip().split("::")
    return long(fields[3]) % 10, Rating(int(fields[0]), int(fields[1]), float(fields[2]))

# Parses a movie record in MovieLens format movieId::movieTitle .
def parseMovie(line):
    fields = line.strip().split("::")
    return int(fields[0]), fields[1]

  
# ratings is an RDD of (last digit of timestamp, (userId, movieId, rating))
ratings = sc.textFile("/mnt/tardis6/movielens/ratings/ratings.dat").map(lambda l: parseRating(l))

# movies is an Dictionary RDD of (movieId, movieTitle)
movies = dict(sc.textFile("/mnt/tardis6/movielens/movies/movies.dat").map(lambda l: parseMovie(l)).collect())

# personalRatings is an RDD of (last digit of timestamp, (userId, movieId, rating))
personalRatings = sc.textFile("/mnt/tardis6/movielens/personalRatings/personalRatings.txt").map(lambda l: parseRating(l))


# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ### Calculate Summary Statistics
# MAGIC * Calculate some summary statistics based on the imported datasets

# COMMAND ----------

# Calculate the number of ratings, distinct users, and distinct movies
numRatings = ratings.count()
numUsers = ratings.values().map(lambda r: r[0]).distinct().count()
numRatedMovies = ratings.values().map(lambda r: r[1]).distinct().count()

# Number of movies
numMovies = sum(len(v) for v in movies.itervalues())

# Specify movieId = 1
movieId1 = movies[1]

# Print out the values
print "Got %d ratings from %d users on %d movies." % (numRatings, numUsers, numRatedMovies)
print "There are a total of %d movies with movieId = 1 as '%s'." % (numMovies, movieId1)


# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Create the Training and Test Datasets
# MAGIC * The purpose is to create two data sets based on the full ratings data set to train and test the machine learning models.
# MAGIC * To do this, we will create the a training dataset (to train the model) and a test data set (which we will test against to validate the precision of the model)

# COMMAND ----------

# Randomly split data between training and test 
training, test = ratings.map(lambda x: x[1]).randomSplit([4, 1], 8)

# Calculate summary statistics
numTraining = training.count()
numTest = test.count()

print "Training: %d, test: %d" % (numTraining, numTest)


# COMMAND ----------



# COMMAND ----------

# Import Alternating Least Squares (ALS) collaborative filtering algorithm
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
from math import sqrt
from operator import add

# Compute RMSE (Root Mean Squared Error)
def computeRmse(model, data, n):
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
      .values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))
  
# Build the recommendation model using Alternating Least Squares
rank = 10
numIterations = 5
lambda_ = 0.01
model = ALS.train(training, rank, numIterations, lambda_)

# Compute RMSE
validationRmse = computeRmse(model, validation, numValidation)
print "RMSE (validation) = %f for the model trained with " % validationRmse \
+ "rank = %d, lambda = %.1f, and numIter = %d." % (rank, lambda_, numIterations)
  

# COMMAND ----------

# Import Alternating Least Squares (ALS) collaborative filtering algorithm
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
from math import sqrt
from operator import add

# Compute RMSE (Root Mean Squared Error)
def computeRmse(model, data, n):
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
      .values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))
  
# Build the recommendation model using Alternating Least Squares
rank = 10
numIterations = 5
lambda_ = 0.01
model = ALS.train(training, rank, numIterations, lambda_)

# Compute RMSE
validationRmse = computeRmse(model, test, numTest)
print "RMSE (validation) = %f for the model trained with " % validationRmse \
+ "rank = %d, lambda = %.1f, and numIter = %d." % (rank, lambda_, numIterations)
  

# COMMAND ----------

# Import Alternating Least Squares (ALS) collaborative filtering algorithm
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics


# Build the recommendation model using Alternating Least Squares
rank = 10
numIterations = 10
lambda_ = 0.01
model = ALS.train(training, rank, numIterations, lambda_)

# Get predicted ratings on test existing user-product pairs
testData = test.map(lambda p: (p.user, p.product))
predictions = model.predictAll(testData).map(lambda r: ((r.user, r.product), r.rating))
ratingsTuple = test.map(lambda r: ((r.user, r.product), r.rating))
scoreAndLabels = predictions.join(ratingsTuple).map(lambda tup: tup[1])

# Instantiate regression metrics to compare predicted and actual ratings
metrics = RegressionMetrics(scoreAndLabels)

# Root mean squared error
print("RMSE = %s" % metrics.rootMeanSquaredError)

# R-squared
print("R-squared = %s" % metrics.r2)



# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Determining the best ALS model based on multiple variables
# MAGIC * Calculate the Root Mean Squared Error (RMSE) and R-Squared (r2) for a trained ALS model
# MAGIC * Loop through different iterations of rank, lambda, and number of Iterations

# COMMAND ----------

# Import Alternating Least Squares (ALS) collaborative filtering algorithm
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
import itertools

# Build the recommendation model using Alternating Least Squares
ranks = [8, 12]
lambdas_ = [0.01, 0.1]
numIterations = [10, 20]
bestModel = None
bestPredictions = None
bestRatingsTuple = None
bestScoreAndLabels = None
bestMetrics = None
bestTestRmse = float("inf")
bestTestR2 = float("inf")
bestRank = 0
bestLambda_ = -1.0
bestNumIteration = -1

# Iterations
print ("Evaluating ALS Model variables:")
for rank, lambda_, numIteration in itertools.product(ranks, lambdas_, numIterations):
  model = ALS.train(training, rank, numIteration, lambda_)

  # Get predicted ratings on test existing user-product pairs
  testData = test.map(lambda p: (p.user, p.product))
  predictions = model.predictAll(testData).map(lambda r: ((r.user, r.product), r.rating))
  ratingsTuple = test.map(lambda r: ((r.user, r.product), r.rating))
  scoreAndLabels = predictions.join(ratingsTuple).map(lambda tup: tup[1])

  # Instantiate regression metrics to compare predicted and actual ratings
  metrics = RegressionMetrics(scoreAndLabels)

  # Print out RMSE and R2 based on different variables
  testRmse = metrics.rootMeanSquaredError
  testR2 = metrics.r2
  print(" >> Rank: %s, Lambda: %s, Iterations: %s, RMSE: %s, R2: %s" % (rank, lambda_, numIteration, testRmse, testR2))
  
  # Evaluate best model
  if (testRmse < bestTestRmse):
    bestModel = model
    bestPredictions = predictions
    bestRatingsTuple = ratingsTuple
    bestScoreAndLabels = scoreAndLabels
    bestMetrics = metrics
    bestTestRmse = testRmse
    bestTestR2 = testR2
    bestRank = rank
    bestLambda_ = lambda_
    bestNumIteration = numIteration
  
# Print out best model
print ("Best model:: Rank: %s, Lambda: %s, Iterations: %s, RMSE: %s, R2: %s" % (bestRank, bestLambda_, bestNumIteration, bestTestRmse, bestTestR2))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Choosing the best ALS Model 
# MAGIC * Calculate the Root Mean Squared Error (RMSE) and R-Squared (r2) for a trained ALS model
# MAGIC * Parameters: Rank = 8, Number of Iterations = 30, Lambda = 0.01

# COMMAND ----------

# Import Alternating Least Squares (ALS) collaborative filtering algorithm
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
import itertools

# Create the model
model = ALS.train(training, bestRank, bestNumIteration, bestLambda_)

# Get predicted ratings on test existing user-product pairs
testData = test.map(lambda p: (p.user, p.product))
predictions = model.predictAll(testData).map(lambda r: ((r.user, r.product), r.rating))
ratingsTuple = test.map(lambda r: ((r.user, r.product), r.rating))
scoreAndLabels = predictions.join(ratingsTuple).map(lambda tup: tup[1])

# Instantiate regression metrics to compare predicted and actual ratings
metrics = RegressionMetrics(scoreAndLabels)

# COMMAND ----------

personalData = personalRatings.map(lambda p: (p.user, p.product))

# COMMAND ----------

def computeRmse(model, data, n):
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
      .values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))
  

# COMMAND ----------

metrics.rootMeanSquaredError
# metrics.r2

# COMMAND ----------

movies[2]

# COMMAND ----------

personalRatings.take(10)

# COMMAND ----------

#set([x[1] for x in personalRatings])
personalRatings[1]

# COMMAND ----------

predictions.take(10)

# COMMAND ----------

scoreAndLabels.take(10)

# COMMAND ----------

