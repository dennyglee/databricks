# Databricks notebook source
# MAGIC %md # Introduction to Neural Networks, MLflow, and SHAP
# MAGIC 
# MAGIC **Purpose**: Introduces the concepts of neural networks and MLflow.  We will train a simple convolutional neural network on the MNIST dataset using Keras (Tensorflow backend) using [Databricks Runtime for Machine Learning](https://databricks.com/blog/2018/06/05/distributed-deep-learning-made-simple.html).  For more information, check out the [Deep Learning Fundamentals Series](https://databricks.com/tensorflow/deep-learning).
# MAGIC 
# MAGIC **Sources**: 
# MAGIC * [`keras/examples/mnist_cnn.py`](https://github.com/keras-team/keras/blob/master/examples/mnist_cnn.py)
# MAGIC * [`shap`](https://github.com/slundberg/shap)

# COMMAND ----------

import shap

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

import keras
from keras.datasets import mnist
from keras.models import Sequential
from keras.layers import Dense, Dropout, Flatten
from keras.layers import Conv2D, MaxPooling2D
from keras import backend as K


# Use TensorFlow Backend
import tensorflow as tf
tf.set_random_seed(42) # For reproducibility

# Print out Keras version
print(keras.__version__)

# COMMAND ----------

# Configure MLflow Experiment
#mlflow_experiment_id = 2102416

# Including MLflow
import mlflow
import mlflow.keras
import os
print("MLflow Version: %s" % mlflow.__version__)

# COMMAND ----------

# MAGIC %md ## Source Data: MNIST
# MAGIC These set of cells are based on the TensorFlow's [MNIST for ML Beginners](https://www.tensorflow.org/versions/r0.9/tutorials/mnist/beginners/index.html). 
# MAGIC 
# MAGIC In reference to `from keras.datasets import mnist` in the previous cell:
# MAGIC 
# MAGIC The purpose of this notebook is to use Keras (with TensorFlow backend) to **automate the identification of handwritten digits** from the  [MNIST Database of Handwritten Digits](http://yann.lecun.com/exdb/mnist/) database. The source of these handwritten digits is from the National Institute of Standards and Technology (NIST) Special Database 3 (Census Bureau employees) and Special Database 1 (high-school students).
# MAGIC 
# MAGIC <img src="https://github.com/dennyglee/databricks/blob/master/images/mnist.png?raw=true" width="300"/>

# COMMAND ----------

# -----------------------------------------------------------
# Hyperparameters
batch_size = 128
num_classes = 10
epochs = 12


# -----------------------------------------------------------
# Image Datasets

# input image dimensions
img_rows, img_cols = 28, 28

# the data, split between train and test sets
(x_train, y_train), (x_test, y_test) = mnist.load_data()

if K.image_data_format() == 'channels_first':
    x_train = x_train.reshape(x_train.shape[0], 1, img_rows, img_cols)
    x_test = x_test.reshape(x_test.shape[0], 1, img_rows, img_cols)
    input_shape = (1, img_rows, img_cols)
else:
    x_train = x_train.reshape(x_train.shape[0], img_rows, img_cols, 1)
    x_test = x_test.reshape(x_test.shape[0], img_rows, img_cols, 1)
    input_shape = (img_rows, img_cols, 1)

x_train = x_train.astype('float32')
x_test = x_test.astype('float32')
x_train /= 255
x_test /= 255
print('x_train shape:', x_train.shape)
print(x_train.shape[0], 'train samples')
print(x_test.shape[0], 'test samples')

# convert class vectors to binary class matrices
y_train = keras.utils.to_categorical(y_train, num_classes)
y_test = keras.utils.to_categorical(y_test, num_classes)

# COMMAND ----------

# MAGIC %md ## What is the image?
# MAGIC Within this dataset, this 28px x 28px 3D structure has been flattened into an array of size 784. 
# MAGIC 
# MAGIC * `x_` contains the handwritten digit 
# MAGIC * `y_` contains the labels
# MAGIC * `_train` contains the 60,000 training samples
# MAGIC * `_test` contains the 10,000 test samples
# MAGIC 
# MAGIC 
# MAGIC For example, if you take the `25168`th element, the label for it is `y_train[25168,:]` indicates its the value `9`.
# MAGIC 
# MAGIC &nbsp;

# COMMAND ----------

# One-Hot Vector for y_train = 25168 representing the number 9 
#  The nth-digit will be represented as a vector which is 1 in the nth dimensions. 
y_train[25168,:]

# COMMAND ----------

# MAGIC %md
# MAGIC `x_train[25168,:]` is the array of 784 digits numerically representing the handwritten digit number `9`.
# MAGIC 
# MAGIC &nbsp;

# COMMAND ----------

from __future__ import print_function

# This is the extracted array for x_train = 25168 from the training matrix
xt_25168 = x_train[25168,:]

print(xt_25168)

# COMMAND ----------

# MAGIC %md
# MAGIC Let's print it as 28 x 28
# MAGIC 
# MAGIC &nbsp;

# COMMAND ----------

# As this is a 28 x 28 image, let's print it out this way
txt = ""
for i in range (0, 27):
   for j in range(0, 27):
      val = "%.3f" % xt_25168[i,j]
      txt += str(val).replace("[", "").replace("]", "") + ", "
   
   print(txt)
   txt = ""

# COMMAND ----------

# MAGIC %md 
# MAGIC You can sort of see the number **9** in there, but let's add a color-scale (the higher the number, the darker the value), you will get the following matrix:
# MAGIC 
# MAGIC <img src="https://dennyglee.files.wordpress.com/2018/09/nine.png" width=500/>
# MAGIC 
# MAGIC Here, you can access the [full-size version](https://dennyglee.files.wordpress.com/2018/09/nine.png) of this image.

# COMMAND ----------

# MAGIC %md ## Oh where art thou GPU?
# MAGIC 
# MAGIC Or **[How can I run Keras on GPU?](https://keras.io/getting-started/faq/#how-can-i-run-keras-on-gpu)**: If you are running on the TensorFlow backends, your code will automatically run on GPU if any available GPU is detected.

# COMMAND ----------

# Check for any available GPUs
K.tensorflow_backend._get_available_gpus()

# COMMAND ----------

from tensorflow.python.client import device_lib
print(device_lib.list_local_devices())

# COMMAND ----------

# MAGIC %md ## How fast did you say?
# MAGIC 
# MAGIC | Processor | Duration |
# MAGIC | --------- | -------- |
# MAGIC | GPU       | 1.87min  |
# MAGIC | CPU       | 23.08min |

# COMMAND ----------

# MAGIC %md ## Convolutional Neural Networks
# MAGIC ![](https://dennyglee.files.wordpress.com/2018/09/keras-cnn-activate.png)
# MAGIC 
# MAGIC 1. The input layer is a grey scale image of 28x28 pixels. 
# MAGIC 2. The first convolution layer maps one grayscale image to 32 feature maps using the activation function
# MAGIC 3. The second convolution layer maps the image to 64 feature maps using the activation function
# MAGIC 4. The pooling layer down samples image by 2x so you have a 14x14 matrix 
# MAGIC 5. The first dropout layer delete random neurons (regularization technique to avoid overfitting)
# MAGIC 6. The fully connected feed-forward maps the features with 128 neurons in the hidden layer
# MAGIC 7. The second dropout layer delete random neurons (regularization technique to avoid overfitting)
# MAGIC 8. Apply `softmax` with 10 hidden layers to identify digit.

# COMMAND ----------

def runCNN(activation, verbose):
  # Building up our CNN
  model = Sequential()
  
  # Convolution Layer
  model.add(Conv2D(32, kernel_size=(3, 3),
                 activation=activation,
                 input_shape=input_shape)) 
  
  # Convolution layer
  model.add(Conv2D(64, (3, 3), activation=activation))
  
  # Pooling with stride (2, 2)
  model.add(MaxPooling2D(pool_size=(2, 2)))
  
  # Delete neuron randomly while training (remain 75%)
  #   Regularization technique to avoid overfitting
  model.add(Dropout(0.25))
  
  # Flatten layer 
  model.add(Flatten())
  
  # Fully connected Layer
  model.add(Dense(128, activation=activation))
  
  # Delete neuron randomly while training (remain 50%) 
  #   Regularization technique to avoid overfitting
  model.add(Dropout(0.5))
  
  # Apply Softmax
  model.add(Dense(num_classes, activation='softmax'))

  # Log MLflow
  #with mlflow.start_run(experiment_id = mlflow_experiment_id) as run:
  with mlflow.start_run() as run:
  
    # Loss function (crossentropy) and Optimizer (Adadelta)
    model.compile(loss=keras.losses.categorical_crossentropy,
              optimizer=keras.optimizers.Adadelta(),
              metrics=['accuracy'])

    # Fit our model
    model.fit(x_train, y_train,
          batch_size=batch_size,
          epochs=epochs,
          verbose=verbose,
          validation_data=(x_test, y_test))

    # Evaluate our model
    score = model.evaluate(x_test, y_test, verbose=0)

    # Log Parameters
    mlflow.log_param("activation function", activation)
    mlflow.log_metric("test loss", score[0])
    mlflow.log_metric("test accuracy", score[1])
    
    # Log Model
    mlflow.keras.log_model(model, "model")
    
  # Return
  return score

# COMMAND ----------

# MAGIC %md ### Using sigmoid

# COMMAND ----------

score_sigmoid = runCNN('sigmoid', 0)
print('Test loss:', score_sigmoid[0])
print('Test accuracy:', score_sigmoid[1])

# COMMAND ----------

# MAGIC %md ### Using tanh

# COMMAND ----------

score_tanh = runCNN('tanh', 0)
print('Test loss:', score_tanh[0])
print('Test accuracy:', score_tanh[1])

# COMMAND ----------

# MAGIC %md ### Using ReLU

# COMMAND ----------

# Building up our CNN
model = Sequential()

# Convolution Layer
model.add(Conv2D(32, kernel_size=(3, 3),
               activation='relu',
               input_shape=input_shape)) 

# Convolution layer
model.add(Conv2D(64, (3, 3), activation='relu'))

# Pooling with stride (2, 2)
model.add(MaxPooling2D(pool_size=(2, 2)))

# Delete neuron randomly while training (remain 75%)
#   Regularization technique to avoid overfitting
model.add(Dropout(0.25))

# Flatten layer 
model.add(Flatten())

# Fully connected Layer
model.add(Dense(128, activation='relu'))

# Delete neuron randomly while training (remain 50%) 
#   Regularization technique to avoid overfitting
model.add(Dropout(0.5))

# Apply Softmax
model.add(Dense(num_classes, activation='softmax'))

# Log MLflow
#with mlflow.start_run(experiment_id = mlflow_experiment_id) as run:
with mlflow.start_run() as run:

  # Loss function (crossentropy) and Optimizer (Adadelta)
  model.compile(loss=keras.losses.categorical_crossentropy,
            optimizer=keras.optimizers.Adadelta(),
            metrics=['accuracy'])

  # Fit our model
  model.fit(x_train, y_train,
        batch_size=batch_size,
        epochs=epochs,
        verbose=1,
        validation_data=(x_test, y_test))

  # Evaluate our model
  score = model.evaluate(x_test, y_test, verbose=0)

  # Log Parameters
  mlflow.log_param("activation function", 'relu')
  mlflow.log_metric("test loss", score[0])
  mlflow.log_metric("test accuracy", score[1])

  # Log Model
  mlflow.keras.log_model(model, "model")

# COMMAND ----------

print('Test loss:', score[0])
print('Test accuracy:', score[1])

# COMMAND ----------

# MAGIC %md
# MAGIC If you are using the `demo` cluster, click here to [compare the three different models](https://demo.cloud.databricks.com/#mlflow/compare-runs?runs=[%222bb5815d2f564768b09228eb8156f89b%22,%22f4e44dba760c4903a01f7c0de13e059d%22,%2265348b89316d4e5bae5fc8bf8fac02f6%22]&experiment=2102416).  
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/introduction-to-neural-networks-and-mlflow.png)

# COMMAND ----------

# MAGIC %md ## Deep learning example with DeepExplainer (TensorFlow/Keras models)
# MAGIC 
# MAGIC Deep SHAP is a high-speed approximation algorithm for SHAP values in deep learning models that builds on a connection with [DeepLIFT](https://arxiv.org/abs/1704.02685) described in the SHAP NIPS paper. The implementation here differs from the original [DeepLIFT](https://arxiv.org/abs/1704.02685) by using a distribution of background samples instead of a single reference value, and using Shapley equations to linearize components such as max, softmax, products, divisions, etc. Note that some of these enhancements have also been since integrated into DeepLIFT. TensorFlow models and Keras models using the TensorFlow backend are currently supported.

# COMMAND ----------

#import shap
import numpy as np

# select a set of background examples to take an expectation over
background = x_train[np.random.choice(x_train.shape[0], 100, replace=False)]

# explain predictions of the model on three images
e = shap.DeepExplainer(model, background)
# ...or pass tensors directly
# e = shap.DeepExplainer((model.layers[0].input, model.layers[-1].output), background)
shap_values = e.shap_values(x_test[1:10])

# COMMAND ----------

# plot the feature attributions
shap_plot = shap.image_plot(shap_values, -x_test[1:5])
display(shap_plot)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC The plot above shows the explanations for each class on four predictions (of the four different images of 2, 1, 0, 4). Note that the explanations are ordered for the classes 0-9 going left to right along the rows, starting with the original image:
# MAGIC 
# MAGIC * Red pixels increase the model's output 
# MAGIC * Blue pixels decrease the model's output. 
# MAGIC 
# MAGIC The input images are shown on the left, and as nearly transparent grayscale backings behind each of the explanations. 
# MAGIC 
# MAGIC The sum of the SHAP values equals the difference between the expected model output (averaged over the background dataset) and the current model output. 
# MAGIC 
# MAGIC Some observations:
# MAGIC * For the 'zero' image the blank middle is important (row 3, col 2)
# MAGIC 
# MAGIC ![shap 0](https://pages.databricks.com/rs/094-YMS-629/images/shap-0.png)
# MAGIC 
# MAGIC 
# MAGIC * For the 'four' image the lack of a connection on top makes it a four instead of a nine (row 4, col 6)
# MAGIC 
# MAGIC ![shap 4](https://pages.databricks.com/rs/094-YMS-629/images/shap-4.png)
# MAGIC 
# MAGIC This is more apparent when looking at the "4" on row 6, col 11 where the blue pixels decrease the model output results
# MAGIC 
# MAGIC ![shap 4.2](https://pages.databricks.com/rs/094-YMS-629/images/shap-4-2.png)

# COMMAND ----------

# plot the feature attributions
shap_plot = shap.image_plot(shap_values, -x_test[1:10])
display(shap_plot)

# COMMAND ----------

# plot the feature attributions
shap_plot = shap.image_plot(shap_values, -x_test[11:20])
display(shap_plot)