# Databricks notebook source exported at Thu, 21 Jul 2016 17:30:32 UTC
# MAGIC %md ## MNIST for ML Beginners
# MAGIC This is the MNIST for ML Beginners using TensorFlow with Databricks and TensorFrames

# COMMAND ----------

# MAGIC %md ### Configuration and Setup
# MAGIC 1. Launch a Spark cluster with version >= 1.6
# MAGIC 2. Attach to this cluster the latest version of the TensorFrames Spark package
# MAGIC 3. In a notebook, run the following command
# MAGIC  * *Ubuntu/Linux 64-bit, CPU only, Python 2.7*
# MAGIC  `/databricks/python/bin/pip install https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow-0.9.0rc0-cp27-none-linux_x86_64.whl`
# MAGIC  * *Ubuntu/Linux 64-bit, GPU enabled, Python 2.7*
# MAGIC  `/databricks/python/bin/pip install https://storage.googleapis.com/tensorflow/linux/gpu/tensorflow-0.9.0rc0-cp27-none-linux_x86_64.whl`
# MAGIC 4. Detach and reattach the notebook you just ran this command from
# MAGIC 5. Your cluster is now configured. You can run pure tensorflow programs on the driver, or TensorFrames examples on the whole cluster

# COMMAND ----------

# MAGIC %sh
# MAGIC /databricks/python/bin/pip install https://storage.googleapis.com/tensorflow/linux/cpu/tensorflow-0.9.0rc0-cp27-none-linux_x86_64.whl

# COMMAND ----------

# MAGIC %md 
# MAGIC ### MNIST for ML Beginners
# MAGIC This set of cells is based on the [MNIST for ML Beginners](https://www.tensorflow.org/versions/r0.9/tutorials/mnist/beginners/index.html). 
# MAGIC 
# MAGIC The purpose of this notebook is to use TensorFrames and Neural Networks to **automate the identification of handwritten digits** from the  [MNIST Database of Handwritten Digits](http://yann.lecun.com/exdb/mnist/) database. The source of these handwritten digits is from the National Institute of Standards and Technology (NIST) Special Database 3 (Census Bureau employees) and Special Database 1 (high-school students).
# MAGIC 
# MAGIC ![](https://www.tensorflow.org/versions/r0.9/images/MNIST.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Import the Dataset
# MAGIC The MNIST dataset is comprised of:
# MAGIC * `mnist.train`: 55,000 data points of training data 
# MAGIC * `mnist.test`: 10,000 points of test data
# MAGIC * `mnist.validation`: 5,000 points of validation data

# COMMAND ----------

# Import MNIST digit images data
from tensorflow.examples.tutorials.mnist import input_data
mnist = input_data.read_data_sets("MNIST_data/", one_hot=True)


# COMMAND ----------

# MAGIC %md ### What is the image?
# MAGIC Within this dataset, this 28px x 28px 3D structure has been flattened into an array of size 784. For this tutorial, we're using a simple algorithm - `softmax regression` which does not actually make use of the 3D structure so we do not lose any information by flattening it 2D. The `.images` contains the [x,784] matrix of representing the digits while the `.labels` contain the `One-Hot Vector` representing the actual number.
# MAGIC 
# MAGIC For example, `mnist.train.images[25138,:]` is the array of 784 digits for the handwritten digit number `9` as indicated in `mnist.train.labels[25138,:]`.

# COMMAND ----------

# One-Hot Vector for xs = 25138 representing the number 9 
#  The nth-digit will be represented as a vector which is 1 in the nth dimensions. 
mnist.train.labels[25138,:]

# COMMAND ----------

# This is the extracted array for xs = 25138 from the training matrix
mnist.train.images[25138,:]

# COMMAND ----------

# MAGIC %md 
# MAGIC But because the output is 5 columns, its really hard to see that is the number **9**.  
# MAGIC 
# MAGIC If you were to take this 11,281 x 5 matrix and convert it back to a 28 x 28 matrix and add a color-scale (the higher the number, the darker the value), you will get this matrix:
# MAGIC 
# MAGIC ![](https://dennyglee.files.wordpress.com/2016/06/unflattened-digit-9-small.png)
# MAGIC 
# MAGIC Here, you can access the [full-size version](https://dennyglee.files.wordpress.com/2016/06/unflattened-digit-9-full.png) of this image.

# COMMAND ----------

# MAGIC %md ## Digit Prediction
# MAGIC 
# MAGIC For those notebook on MNIST digit prediction, we will use the Softmax Regressions model. 
# MAGIC For more information, please reference the [softmax regression](https://www.tensorflow.org/versions/r0.9/tutorials/mnist/beginners/index.html#softmax-regressions) analysis.

# COMMAND ----------

# MAGIC %md #### Implementing Softmax Regressions model

# COMMAND ----------

# Import TensorFlow
import tensorflow as tf

# Create `x` placeholder
#   Place any any number of MNIST images, each flattened into a 784-dimensional vector
#   This is represented  as a 2-D tensor of floating-point numbers, with a shape [None, 784]
x = tf.placeholder(tf.float32, [None, 784])

# Set the weights (`W`) and biases (`b`) for our model
#   Use a Variable - a modificable tensor that lives in TensorFlow's graph of interacting operations
#   We initalize them them with `zeros`
W = tf.Variable(tf.zeros([784, 10]))
b = tf.Variable(tf.zeros([10]))

# Implement Softmax Regressions model
y = tf.nn.softmax(tf.matmul(x, W) + b)

# COMMAND ----------

# MAGIC %md #### Training the model
# MAGIC Use the `cross-entropy` cost function to define what it means for the model to be good.  For more information, please reference [MNIST for Beginners > Training](https://www.tensorflow.org/versions/r0.9/tutorials/mnist/beginners/index.html#training)

# COMMAND ----------

# Create `y_` placeholder Variable to input correct answers
y_ = tf.placeholder(tf.float32, [None, 10])

# Implement the `cross-entopy` cost function
cross_entropy = tf.reduce_mean(-tf.reduce_sum(y_ * tf.log(y), reduction_indices=[1]))

# Traing using back-propagation (gradient descent optimizer)
train_step = tf.train.GradientDescentOptimizer(0.5).minimize(cross_entropy)

# Initialize all variables 
init = tf.initialize_all_variables()

# Launch the model in a `Session`, and run the operation that initializes the variables
sess = tf.Session()
sess.run(init)

# Let's train -- running the training step 1000 times
for i in range(1000):
  batch_xs, batch_ys = mnist.train.next_batch(100)
  sess.run(train_step, feed_dict={x: batch_xs, y_: batch_ys})

# COMMAND ----------

# MAGIC %md ### Evaluating our Model
# MAGIC Determine how well the model worked using the `tf.argmax` function which provides the index of the highest entry in a tensor along the specified axis.
# MAGIC 
# MAGIC * `tf.argmax(y,1)` is the predicted label (i.e. what number the model thinks it is)
# MAGIC * `tf.argmax(y_,1)` is the actual label (i.e. what number it actually is)
# MAGIC 
# MAGIC Note, we use `tf.equal` to check if the predcited value matches the actual value.

# COMMAND ----------

# Is the prediciton correct?  (output is a set of booleans (i.e. T, F, T, T))
correct_prediction = tf.equal(tf.argmax(y,1), tf.argmax(y_,1))

# Determine the accuracy by casting to floating point unit and then take the mean
accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

# Print out the accuracy of the test
print(sess.run(accuracy, feed_dict={x: mnist.test.images, y_: mnist.test.labels}))

# COMMAND ----------

print(y_)