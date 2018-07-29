# Databricks notebook source
# MAGIC %md ## MLflow Quick Start Notebook
# MAGIC This is a Quick Start notebook based on [MLflow's tutorial](https://mlflow.org/docs/latest/tutorial.html).  In this tutorial, we’ll:
# MAGIC * Install the MLflow library on a Databricks cluster
# MAGIC * Connect our notebook to an MLflow Tracking Server running on EC2
# MAGIC * Log metrics, parameters, models and a .png plot to show how you can record arbitrary outputs from your MLflow job
# MAGIC * View our results on the MLflow tracking UI.
# MAGIC 
# MAGIC This notebook uses the `diabetes` dataset in scikit-learn and predicts the progression metric (a quantitative measure of disease progression after one year after) based on BMI, blood pressure, etc. It uses the scikit-learn ElasticNet linear regression model, where we vary the `alpha` and `l1_ratio` parameters for tuning. For more information on ElasticNet, refer to:
# MAGIC   * [Elastic net regularization](https://en.wikipedia.org/wiki/Elastic_net_regularization)
# MAGIC   * [Regularization and Variable Selection via the Elastic Net](https://web.stanford.edu/~hastie/TALKS/enet_talk.pdf)
# MAGIC 
# MAGIC A good reference for MLflow in general is [Matei's Spark Summit 2018 Keynote](https://databricks.com/sparkaisummit/north-america/spark-summit-2018-keynotes).
# MAGIC 
# MAGIC To get started, you will first need to 
# MAGIC 
# MAGIC 1. Install MLflow on your Databricks cluster and 
# MAGIC 2. Set up a Remote MLflow Tracking Server
# MAGIC 
# MAGIC This notebook can also be found at [MLflow Quick Start Notebook](https://docs.databricks.com/_static/notebooks/mlflow/mlflow-quick-start-notebook.html) in [Databricks Documentation](https://docs.databricks.com/)

# COMMAND ----------

# MAGIC %md ### Install MLflow on Your Databricks Cluster
# MAGIC 
# MAGIC 1. Ensure you are using or [create a cluster](https://docs.databricks.com/user-guide/clusters/create.html) specifying 
# MAGIC   * **Databricks Runtime Version:** Databricks Runtime 4.1 
# MAGIC   * **Python Version:** Python 3
# MAGIC 2. Add `mlflow` as a PyPi library in Databricks, and install it on your cluster
# MAGIC   * Follow [Upload a Python PyPI package or Python Egg](https://docs.databricks.com/user-guide/libraries.html#upload-a-python-pypi-package-or-python-egg) to create a library
# MAGIC   * Choose **PyPi** and enter `mlflow`
# MAGIC   * This notebook was tested with `mlflow` version 0.2.1
# MAGIC 3. For our ElasticNet Descent Path visualizations, install the latest version `scikit-learn` and `matplotlib` as a PyPi library in Databricks
# MAGIC   * Follow [Upload a Python PyPI package or Python Egg](https://docs.databricks.com/user-guide/libraries.html#upload-a-python-pypi-package-or-python-egg) to create a library
# MAGIC   * Choose **PyPi** and enter `scikit-learn==0.19.1`
# MAGIC   * Repeat for matplotlib `matplotlib==2.2.2`

# COMMAND ----------

# MAGIC %md ### Set up a Remote MLflow Tracking Server
# MAGIC 
# MAGIC To run a long-lived, shared MLflow tracking server, we'll launch an EC2 instance to run the [MLflow Tracking server](https://mlflow.org/docs/latest/tracking.html). To do this:
# MAGIC 
# MAGIC * Create an **Anaconda with Python 3** AMI EC2 instance 
# MAGIC   * *You can use a t2.micro (Free-tier) instance for test environment*
# MAGIC   * *This AMI already has `conda` and many other packages needed pre-installed*
# MAGIC   * Open port 5000 for MLflow server; an example of how to do this via [How to open a web server port on EC2 instance
# MAGIC ](https://stackoverflow.com/questions/17161345/how-to-open-a-web-server-port-on-ec2-instance). Opening up port 5000 to the Internet will allow anyone to access your server, so it is recommended to only open up the port within an [AWS VPC](https://aws.amazon.com/vpc/) that your Databricks clusters have access to.
# MAGIC 
# MAGIC * Configure your AWS credentials on the instance
# MAGIC   * The optimal configuration for MLflow Remote Tracking is to use the `artifact-root` option to store your artifacts in an S3 bucket
# MAGIC   * To do this, first SSH into your EC2 instance, e.g. `ssh -i ~/.ssh/$mykey$.pem ec2-user@$HOSTNAME$.us-east-2.compute.amazonaws.com`
# MAGIC   * Then, configure your S3 credentials via `aws cli`; for more information, refer to [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html) 
# MAGIC 
# MAGIC * Run your Tracking Server
# MAGIC   * Run the `server` command in MLflow passing it your `artifact-root` and `--host 0.0.0.0`, e.g. `mlflow server --artifact-root s3://databricks-dennylee/MLflow --host 0.0.0.0`.
# MAGIC     * For more information, refer to [MLflow > Running a Tracking Server](https://mlflow.org/docs/latest/tracking.html?highlight=server#running-a-tracking-server).
# MAGIC   * To test connectivity of your tracking server:
# MAGIC     * Get the hostname of your EC2 instance
# MAGIC     * Go to http://$TRACKING_SERVER$:5000; it should look similar to this [MLflow UI](https://databricks.com/wp-content/uploads/2018/06/mlflow-web-ui.png)
# MAGIC     
# MAGIC * Configure AWS credentials on your Databricks cluster to access the S3 `aritfact-root` you chose
# MAGIC   * Either give your cluster an IAM role that can access the bucket, or follow [How to set an environment variable](https://forums.databricks.com/answers/11128/view.html) to set your AWS credentials which match the ones you used for `mlflow server`
# MAGIC   * If the cluster was already running, restart it for those credentials to kick in

# COMMAND ----------

# MAGIC %md ### Start Using MLflow in a Notebook
# MAGIC 
# MAGIC The first step is to import call `mlflow.set_tracking_uri` to point to your server:

# COMMAND ----------

# Set this variable to your MLflow server's DNS name
mlflow_server = 'ec2-18-219-146-205.us-east-2.compute.amazonaws.com'

# Tracking URI
mlflow_tracking_URI = 'http://' + mlflow_server + ':5000'
print ("MLflow Tracking URI: %s" % (mlflow_tracking_URI))

# Import MLflow and set the Tracking UI
import mlflow
mlflow.set_tracking_uri(mlflow_tracking_URI)

# COMMAND ----------

# MAGIC %md #### Write Your ML Code Based on the`train_diabetes.py` Code
# MAGIC This tutorial is based on the MLflow's [train_diabetes.py](https://github.com/databricks/mlflow/blob/master/example/tutorial/train_diabetes.py), which uses the `sklearn.diabetes` built-in dataset to predict disease progression based on various factors.

# COMMAND ----------

# Import various libraries including matplotlib, sklearn, mlflow
import os
import warnings
import sys

import pandas as pd
import numpy as np
from itertools import cycle
import matplotlib.pyplot as plt
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.linear_model import ElasticNet
from sklearn.linear_model import lasso_path, enet_path
from sklearn import datasets

# Import mlflow
import mlflow
import mlflow.sklearn

# Load Diabetes datasets
diabetes = datasets.load_diabetes()
X = diabetes.data
y = diabetes.target

# Create pandas DataFrame for sklearn ElasticNet linear_model
Y = np.array([y]).transpose()
d = np.concatenate((X, Y), axis=1)
cols = ['age', 'sex', 'bmi', 'bp', 's1', 's2', 's3', 's4', 's5', 's6', 'progression']
data = pd.DataFrame(d, columns=cols)

# COMMAND ----------

# MAGIC %md #### Plot the ElasticNet Descent Path
# MAGIC As an example of recording arbitrary output files in MLflow, we'll plot the [ElasticNet Descent Path](http://scikit-learn.org/stable/auto_examples/linear_model/plot_lasso_coordinate_descent_path.html) for the ElasticNet model by *alpha* for the specified *l1_ratio*.
# MAGIC 
# MAGIC The `plot_enet_descent_path` function below:
# MAGIC * Returns an image that can be displayed in our Databricks notebook via `display`
# MAGIC * As well as saves the figure `ElasticNet-paths.png` to the Databricks cluster's driver node
# MAGIC * This file is then uploaded to MLflow using the `log_artifact` within `train_diabetes`

# COMMAND ----------

def plot_enet_descent_path(X, y, l1_ratio):
    # Compute paths
    eps = 5e-3  # the smaller it is the longer is the path

    # Reference the global image variable
    global image
    
    print("Computing regularization path using the elastic net.")
    alphas_enet, coefs_enet, _ = enet_path(X, y, eps=eps, l1_ratio=l1_ratio, fit_intercept=False)

    # Display results
    fig = plt.figure(1)
    ax = plt.gca()

    colors = cycle(['b', 'r', 'g', 'c', 'k'])
    neg_log_alphas_enet = -np.log10(alphas_enet)
    for coef_e, c in zip(coefs_enet, colors):
        l1 = plt.plot(neg_log_alphas_enet, coef_e, linestyle='--', c=c)

    plt.xlabel('-Log(alpha)')
    plt.ylabel('coefficients')
    title = 'ElasticNet Path by alpha for l1_ratio = ' + str(l1_ratio)
    plt.title(title)
    plt.axis('tight')

    # Display images
    image = fig
    
    # Save figure
    fig.savefig("ElasticNet-paths.png")

    # Close plot
    plt.close(fig)

    # Return images
    return image    

# COMMAND ----------

# MAGIC %md #### Train the Diabetes Model
# MAGIC The next function trains Elastic-Net linear regression based on the input parameters of `alpha (in_alpha)` and `l1_ratio (in_l1_ratio)`.
# MAGIC 
# MAGIC In addition, this function uses MLflow Tracking to record its
# MAGIC * parameters,
# MAGIC * metrics,
# MAGIC * model,
# MAGIC * and arbitrary files, namely the above noted Lasso Descent Path Plot.
# MAGIC 
# MAGIC **Tip on how we use `with mlflow.start_run():` in the Python code to create a new MLflow run.** This is the recommended way to use MLflow in notebook cells. Whether your code completes or exits with an error, the `with` context will make sure that we close the MLflow run, so you don't have to call `mlflow.end_run` later in the code.

# COMMAND ----------

# train_diabetes
#   Uses the sklearn Diabetes dataset to predict diabetes progression using ElasticNet
#       The predicted "progression" column is a quantitative measure of disease progression one year after baseline
#       http://scikit-learn.org/stable/modules/generated/sklearn.datasets.load_diabetes.html
def train_diabetes(data, in_alpha, in_l1_ratio):
  # Evaluate metrics
  def eval_metrics(actual, pred):
      rmse = np.sqrt(mean_squared_error(actual, pred))
      mae = mean_absolute_error(actual, pred)
      r2 = r2_score(actual, pred)
      return rmse, mae, r2

  warnings.filterwarnings("ignore")
  np.random.seed(40)

  # Split the data into training and test sets. (0.75, 0.25) split.
  train, test = train_test_split(data)

  # The predicted column is "progression" which is a quantitative measure of disease progression one year after baseline
  train_x = train.drop(["progression"], axis=1)
  test_x = test.drop(["progression"], axis=1)
  train_y = train[["progression"]]
  test_y = test[["progression"]]

  if float(in_alpha) is None:
    alpha = 0.05
  else:
    alpha = float(in_alpha)
    
  if float(in_l1_ratio) is None:
    l1_ratio = 0.05
  else:
    l1_ratio = float(in_l1_ratio)
  
  # Start an MLflow run; the "with" keyword ensures we'll close the run even if this cell crashes
  with mlflow.start_run():
    lr = ElasticNet(alpha=alpha, l1_ratio=l1_ratio, random_state=42)
    lr.fit(train_x, train_y)

    predicted_qualities = lr.predict(test_x)

    (rmse, mae, r2) = eval_metrics(test_y, predicted_qualities)

    # Print out ElasticNet model metrics
    print("Elasticnet model (alpha=%f, l1_ratio=%f):" % (alpha, l1_ratio))
    print("  RMSE: %s" % rmse)
    print("  MAE: %s" % mae)
    print("  R2: %s" % r2)

    # Set tracking_URI first and then reset it back to not specifying port
    # Note, we had specified this in an earlier cell
    #mlflow.set_tracking_uri(mlflow_tracking_URI)

    # Log mlflow attributes for mlflow UI
    mlflow.log_param("alpha", alpha)
    mlflow.log_param("l1_ratio", l1_ratio)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)
    mlflow.log_metric("mae", mae)
    mlflow.sklearn.log_model(lr, "model")
    
    # Call plot_enet_descent_path
    image = plot_enet_descent_path(X, y, l1_ratio)
    
    # Log artifacts (output files)
    mlflow.log_artifact("ElasticNet-paths.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://docs.databricks.com/_static/images/mlflow/elasticnet-paths-by-alpha-per-l1-ratio.png)

# COMMAND ----------

# MAGIC %md #### Experiment with Different Parameters
# MAGIC 
# MAGIC Now that we have a `train_diabetes` function that records MLflow runs, we can simply call it with different parameters to explore them. Later, we'll be able to visualize all these runs on our MLflow tracking server.

# COMMAND ----------

# Start with alpha and l1_ratio values of 0.01, 0.01
train_diabetes(data, 0.01, 0.01)

# COMMAND ----------

display(image)

# COMMAND ----------

# Start with alpha and l1_ratio values of 0.01, 0.75
train_diabetes(data, 0.01, 0.75)

# COMMAND ----------

display(image)

# COMMAND ----------

# Start with alpha and l1_ratio values of 0.01, 1
train_diabetes(data, 0.01, 1)

# COMMAND ----------

display(image)

# COMMAND ----------

# Start with alpha and l1_ratio values of 0.01, 0.01
train_diabetes(data, 0.01, 0.01)

# COMMAND ----------

display(image)

# COMMAND ----------

# MAGIC %md ## Review the MLflow UI
# MAGIC Open the URL of your tracking server in a web browser. In case you forgot it, you can get it from `mlflow.get_tracking_uri()`:

# COMMAND ----------

# Identify the location of the runs
mlflow.tracking.get_tracking_uri()

# COMMAND ----------

# MAGIC %md
# MAGIC The MLflow UI should look something similar to the animated GIF below. Inside the UI, you can:
# MAGIC * View your experiments and runs
# MAGIC * Review the parameters and metrics on each run
# MAGIC * Click each run for a detailed view to see the the model, images, and other artifacts produced.
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_static/images/mlflow/mlflow-ui.gif"/>

# COMMAND ----------

# MAGIC %md #### Organize MLflow Runs into Experiments
# MAGIC 
# MAGIC As you start using your MLflow server for more tasks, you may want to separate them out. MLflow allows you to create [experiments](https://mlflow.org/docs/latest/tracking.html#organizing-runs-in-experiments) to organize your runs. To report your run to a specific experiment, just pass an `experiment_id` parameter to the `mlflow.start_run`, as in `mlflow.start_run(experiment_id=1)`.