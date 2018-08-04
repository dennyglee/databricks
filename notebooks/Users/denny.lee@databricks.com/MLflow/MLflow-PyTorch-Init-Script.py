# Databricks notebook source
# MAGIC %md # MLflow-PyTorch Init Script
# MAGIC 
# MAGIC Run this notebook to set up an init script that installs PyTorch on a GPU-enabled cluster:
# MAGIC 
# MAGIC 1. If you have not already done so, create a GPU-enabled Databricks cluster:
# MAGIC   * **Databricks Runtime Version:** Databricks Runtime 4.2 **GPU**
# MAGIC   * **Python Version:** Python 3
# MAGIC   
# MAGIC 1. Import this notebook to your workspace.
# MAGIC 
# MAGIC 1. Edit the name of the cluster below (i.e. the `clusterName` variable) to the name of the cluster you intend to use. You only need to do it once for each cluster.
# MAGIC 
# MAGIC 1. Run this script.

# COMMAND ----------

# Configure the clusterName so the script will be installed in the correct DBFS folder
clusterName="MLflow-PyTorch"

# COMMAND ----------

# MAGIC %md ### CUDA Version Tip
# MAGIC * The script in the following cell installs PyTorch 0.4.1, GPU, for CUDA 9.0
# MAGIC * You can run the shell command in this cell directly below to validate the CUDA version

# COMMAND ----------

# MAGIC %sh cat /usr/local/cuda/version.txt

# COMMAND ----------

# MAGIC %md ## Script Definition
# MAGIC This script will install PyTorch and TorchVision and copy the `install-pytorch.sh` script to the `dbfs:/databricks/init/$clusterName$` folder

# COMMAND ----------

script = """#!/usr/bin/env bash

set -ex

echo "**** Installing PyTorch ****"

/databricks/python/bin/pip install http://download.pytorch.org/whl/cu90/torch-0.4.1-cp35-cp35m-linux_x86_64.whl
/databricks/python/bin/pip install torchvision
"""

# COMMAND ----------

dbutils.fs.put("dbfs:/databricks/init/%s/install-pytorch.sh" % clusterName, script, True)

# COMMAND ----------

# MAGIC %md ### Validate Script Installation
# MAGIC You can run the script below to validate if the script was properly installed
# MAGIC * Edit the cluster name from `MLflow-PyTorch` to the name of your cluster
# MAGIC * Uncomment the command
# MAGIC * Execute the cell

# COMMAND ----------

# %fs ls /databricks/init/MLflow-PyTorch/

# COMMAND ----------

