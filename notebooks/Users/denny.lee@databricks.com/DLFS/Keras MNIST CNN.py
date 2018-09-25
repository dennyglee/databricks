# Databricks notebook source
# MAGIC %md # MNIST demo using Keras CNN
# MAGIC 
# MAGIC **Purpose**: Trains a simple ConvNet on the MNIST dataset using Keras using [Databricks Runtime for Machine Learning](https://databricks.com/blog/2018/06/05/distributed-deep-learning-made-simple.html)
# MAGIC 
# MAGIC **Source**: [`keras/examples/mnist_cnn.py`](https://github.com/keras-team/keras/blob/master/examples/mnist_cnn.py)

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

# COMMAND ----------

# MAGIC %md ## Source Data: MNIST
# MAGIC These set of cells are based on the TensorFlow's [MNIST for ML Beginners](https://www.tensorflow.org/versions/r0.9/tutorials/mnist/beginners/index.html). 
# MAGIC 
# MAGIC In reference to `from keras.datasets import mnist` in the previous cell:
# MAGIC 
# MAGIC The purpose of this notebook is to use Keras (with TensorFlow backend) to **automate the identification of handwritten digits** from the  [MNIST Database of Handwritten Digits](http://yann.lecun.com/exdb/mnist/) database. The source of these handwritten digits is from the National Institute of Standards and Technology (NIST) Special Database 3 (Census Bureau employees) and Special Database 1 (high-school students).
# MAGIC 
# MAGIC <img src="https://www.tensorflow.org/versions/r0.9/images/MNIST.png" width="300"/>

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

# MAGIC %md ### Using ReLu

# COMMAND ----------

score_relu = runCNN('relu', 1)

# COMMAND ----------

print('Test loss:', score_relu[0])
print('Test accuracy:', score_relu[1])