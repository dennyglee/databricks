# Databricks notebook source
# MAGIC %md # MLflow PyTorch Notebook
# MAGIC 
# MAGIC This is a MLflow PyTorch notebook is based on [MLflow's PyTorch Tensorboard tutorial](https://github.com/mlflow/mlflow/blob/master/example/tutorial/pytorch_tensorboard.py).  
# MAGIC * This notebook demonstrates how to run PyTorch to fit a neural network on MNIST handwritten digit recognition data.
# MAGIC * The run results are logged to an MLFlow server. 
# MAGIC   * Training metrics and weights in TensorFlow event format are logged locally and then uploaded to the MLflow run's artifact directory.
# MAGIC * TensorBoard is started on the local log and then optionally on the uploaded log.
# MAGIC 
# MAGIC In this tutorial, we will:
# MAGIC * Install the MLflow and Tensorboard libraries on a Databricks cluster
# MAGIC * Set up a remote MLflow Tracking Server
# MAGIC * Install PyTorch on a Databricks cluster
# MAGIC * Run an neural network on MNIST handwritten digit recognition data

# COMMAND ----------

# MAGIC %md ### Install MLflow and TensorFlow on Your Databricks Cluster
# MAGIC 
# MAGIC 1. Ensure you are using or [create a cluster](https://docs.databricks.com/user-guide/clusters/create.html) specifying 
# MAGIC   * **Databricks Runtime Version:** Databricks Runtime 4.2 **GPU**
# MAGIC   * **Python Version:** Python 3
# MAGIC 2. Add `mlflow` as a PyPi library in Databricks, and install it on your cluster
# MAGIC   * Follow [Upload a Python PyPI package or Python Egg](https://docs.databricks.com/user-guide/libraries.html#upload-a-python-pypi-package-or-python-egg) to create a library
# MAGIC   * Choose **PyPi** and enter `mlflow==0.4.0`
# MAGIC   * This notebook was tested with `mlflow` version 0.4.0
# MAGIC 3. Add `tensorflow` as a PyPi library in Databricks, and install it on your cluster
# MAGIC   * Follow [Upload a Python PyPI package or Python Egg](https://docs.databricks.com/user-guide/libraries.html#upload-a-python-pypi-package-or-python-egg) to create a library
# MAGIC   * Choose **PyPi** and enter `tensorflow`

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
# MAGIC   * The optimal configuration for MLflow Remote Tracking is to use the `--default-artifact-root` option to store your artifacts in an S3 bucket
# MAGIC   * To do this, first SSH into your EC2 instance, e.g. `ssh -i ~/.ssh/$mykey$.pem ec2-user@$HOSTNAME$.us-east-2.compute.amazonaws.com`
# MAGIC   * Then, configure your S3 credentials via `aws cli`; for more information, refer to [Configuring the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html) 
# MAGIC 
# MAGIC * Run your Tracking Server
# MAGIC   * Run the `server` command in MLflow passing it your `--default-artifact-root` and `--host 0.0.0.0`, e.g. `mlflow server --default-artifact-root s3://databricks-dennylee/MLflow --host 0.0.0.0`.
# MAGIC     * For more information, refer to [MLflow > Running a Tracking Server](https://mlflow.org/docs/latest/tracking.html?highlight=server#running-a-tracking-server).
# MAGIC   * To test connectivity of your tracking server:
# MAGIC     * Get the hostname of your EC2 instance
# MAGIC     * Go to http://$TRACKING_SERVER$:5000; it should look similar to this [MLflow UI](https://databricks.com/wp-content/uploads/2018/06/mlflow-web-ui.png)
# MAGIC     
# MAGIC * Configure AWS credentials on your Databricks cluster to access the S3 `--default-artifact-root` you chose
# MAGIC   * Either give your cluster an IAM role that can access the bucket, or follow [How to set an environment variable](https://forums.databricks.com/answers/11128/view.html) to set your AWS credentials which match the ones you used for `mlflow server`
# MAGIC   * If the cluster was already running, restart it for those credentials to kick in

# COMMAND ----------

# MAGIC %md ### Install PyTorch
# MAGIC To do this, you will execute the [MLflow-PyTorch-Init-Script] notebook per the following instructions:
# MAGIC * Detach this `MLflow-PyTorch` notebook from your cluster
# MAGIC * Attach the `MLflow-PyTorch-Init-Script` notebook to your cluster, *follow its instructions*, and then execute it
# MAGIC   * It will install the `install-pytorch.sh` script onto the `dbfs:/databricks/init/$clustername$/` folder.
# MAGIC * **Edit** the cluster:
# MAGIC   * Keep the same name
# MAGIC   * Change the cluster configuration to GPU, this may require you to change the *driver type* and *worker type*
# MAGIC * Restart the cluster
# MAGIC   * Because the `install-pytorch.sh` script was installed in the `dbfs:/databricks/init/$clustername$/` folder, the restart of the cluster will execute the `install-pytorch.sh` and install `pytorch`
# MAGIC   * You can validate this after the install by running `import torch` in a cell of an attached notebook
# MAGIC * Re-attach this `MLflow-PyTorch` notebook and proceed to run the execution cells.

# COMMAND ----------

# MAGIC %md # Configure an MLflow tracking server
# MAGIC 
# MAGIC See `Cmd3` on how to set up the server; for more information, refer to [MLflow Quick Start Notebook](https://docs.databricks.com/spark/latest/mllib/mlflow.html#mlflow-mlflow-quick-start-notebook).

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

# MAGIC %md # Train an MNIST digit recognizer using PyTorch

# COMMAND ----------

# Trains using PyTorch and logs training metrics and weights in TensorFlow event format to the MLflow run's artifact directory. 
# This stores the TensorFlow events in MLflow for later access using TensorBoard.
#
# Code based on https://github.com/mlflow/mlflow/blob/master/example/tutorial/pytorch_tensorboard.py.
#

from __future__ import print_function
import os
import mlflow
import tempfile
import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torchvision import datasets, transforms
from torch.autograd import Variable
#from tensorboardX import SummaryWriter
from collections import namedtuple
import tensorflow as tf
import tensorflow.summary
from tensorflow.summary import scalar
from tensorflow.summary import histogram
from chardet.universaldetector import UniversalDetector

# Vars() doesn't work on this due to changes from Python 3.5.1 onwards per https://stackoverflow.com/questions/34166469/did-something-about-namedtuple-change-in-3-5-1
#Params = namedtuple('Params', ['batch_size', 'test_batch_size', 'epochs', 'lr', 'momentum', 'seed', 'cuda', 'log_interval'])
#args = Params(batch_size=64, test_batch_size=1000, epochs=10, lr=0.01, momentum=0.5, seed=1, cuda=False, log_interval=200)

# Create Params dictionary
class Params(object):
	def __init__(self, batch_size, test_batch_size, epochs, lr, momentum, seed, cuda, log_interval):
		self.batch_size = batch_size
		self.test_batch_size = test_batch_size
		self.epochs = epochs
		self.lr = lr
		self.momentum = momentum
		self.seed = seed
		self.cuda = cuda
		self.log_interval = log_interval

# Configure args
args = Params(64, 1000, 10, 0.01, 0.5, 1, False, 200)

cuda = not args.cuda and torch.cuda.is_available()


kwargs = {'num_workers': 1, 'pin_memory': True} if cuda else {}
train_loader = torch.utils.data.DataLoader(
    datasets.MNIST('../data', train=True, download=True,
                   transform=transforms.Compose([
                       transforms.ToTensor(),
                       transforms.Normalize((0.1307,), (0.3081,))
                   ])),
    batch_size=args.batch_size, shuffle=True, **kwargs)
test_loader = torch.utils.data.DataLoader(
    datasets.MNIST('../data', train=False, transform=transforms.Compose([
                       transforms.ToTensor(),
                       transforms.Normalize((0.1307,), (0.3081,))
                   ])),
    batch_size=args.batch_size, shuffle=True, **kwargs)

class Net(nn.Module):
    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 10, kernel_size=5)
        self.conv2 = nn.Conv2d(10, 20, kernel_size=5)
        self.conv2_drop = nn.Dropout2d()
        self.fc1 = nn.Linear(320, 50)
        self.fc2 = nn.Linear(50, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 2))
        x = F.relu(F.max_pool2d(self.conv2_drop(self.conv2(x)), 2))
        x = x.view(-1, 320)
        x = F.relu(self.fc1(x))
        x = F.dropout(x, training=self.training)
        x = self.fc2(x)
        return F.log_softmax(x, dim=0)

    def log_weights(self, step):
        writer.add_summary(histogram('weights/conv1/weight', model.conv1.weight.data).eval(), step)
        writer.add_summary(histogram('weights/conv1/bias', model.conv1.bias.data).eval(), step)
        writer.add_summary(histogram('weights/conv2/weight', model.conv2.weight.data).eval(), step)
        writer.add_summary(histogram('weights/conv2/bias', model.conv2.bias.data).eval(), step)
        writer.add_summary(histogram('weights/fc1/weight', model.fc1.weight.data).eval(), step)
        writer.add_summary(histogram('weights/fc1/bias', model.fc1.bias.data).eval(), step)
        writer.add_summary(histogram('weights/fc2/weight', model.fc2.weight.data).eval(), step)
        writer.add_summary(histogram('weights/fc2/bias', model.fc2.bias.data).eval(), step)

model = Net()
if cuda:
    model.cuda()

optimizer = optim.SGD(model.parameters(), lr=args.lr, momentum=args.momentum)

writer = None # Will be used to write TensorBoard events

def train(epoch):
    model.train()
    for batch_idx, (data, target) in enumerate(train_loader):
        if cuda:
            data, target = data.cuda(), target.cuda()
        data, target = Variable(data), Variable(target)
        optimizer.zero_grad()
        output = model(data)
        loss = F.nll_loss(output, target)
        loss.backward()
        optimizer.step()
        if batch_idx % args.log_interval == 0:
            print('Train Epoch: {} [{}/{} ({:.0f}%)]\tLoss: {:.6f}'.format(
                epoch, batch_idx * len(data), len(train_loader.dataset),
                100. * batch_idx / len(train_loader), loss.data.item()))
            step = epoch * len(train_loader) + batch_idx
            log_scalar('train_loss', loss.data.item(), step)
            model.log_weights(step)

def test(epoch):
    model.eval()
    test_loss = 0
    correct = 0
    with torch.no_grad():
        for data, target in test_loader:
            if cuda:
                data, target = data.cuda(), target.cuda()
            data, target = Variable(data), Variable(target)
            output = model(data)
            test_loss += F.nll_loss(output, target, reduction='sum').data.item() # sum up batch loss
            pred = output.data.max(1)[1] # get the index of the max log-probability
            correct += pred.eq(target.data).cpu().sum().item()

    test_loss /= len(test_loader.dataset)
    test_accuracy = 100.0 * correct / len(test_loader.dataset)
    print('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n'.format(
        test_loss, correct, len(test_loader.dataset), test_accuracy))
    step = (epoch + 1) * len(train_loader)
    log_scalar('test_loss', test_loss, step)
    log_scalar('test_accuracy', test_accuracy, step)

def log_scalar(name, value, step):
    """Log a scalar value to both MLflow and TensorBoard"""
    writer.add_summary(scalar(name, value).eval(), step)
    mlflow.log_metric(name, value)

# COMMAND ----------

# MAGIC %md # Create a TensorFlow session and start MLFlow

# COMMAND ----------

sess = tf.InteractiveSession()
with mlflow.start_run():  
  # Log our parameters into mlflow
  for key, value in vars(args).items():
      mlflow.log_param(key, value)

  output_dir = tempfile.mkdtemp()
  print("Writing TensorFlow events locally to %s\n" % output_dir)
  writer = tf.summary.FileWriter(output_dir, graph=sess.graph) 
  
  for epoch in range(1, args.epochs + 1):
      # print out active_run
      print("Active Run ID: %s, Epoch: %s \n" % (mlflow.active_run(), epoch))
      
      train(epoch)
      test(epoch)

# COMMAND ----------

# MAGIC %md ## Save the TensorFlow event log as a run artifact

# COMMAND ----------

print("Uploading TensorFlow events as a run artifact.")
mlflow.log_artifacts(output_dir, artifact_path="events")

# COMMAND ----------

# Finish run
mlflow.end_run(status='FINISHED')

# COMMAND ----------

# MAGIC %md ## Start TensorBoard on local directory

# COMMAND ----------

dbutils.tensorboard.start(output_dir)

# COMMAND ----------

# MAGIC %md #### MLflow UI for the recently executed PyTorch MNIST Run
# MAGIC <img src="https://s3.us-east-2.amazonaws.com/databricks-dennylee/media/MLflow-PyTorch-MLflow-UI.gif" width=1000/>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md #### Tensorboard for the recently executed PyTorch MNIST Run
# MAGIC <img src="https://s3.us-east-2.amazonaws.com/databricks-dennylee/media/MLflow-PyTorch-Tensorboard.gif" width=1000/>

# COMMAND ----------

# MAGIC %md ## Stop TensorBoard

# COMMAND ----------

dbutils.tensorboard.stop()

# COMMAND ----------

