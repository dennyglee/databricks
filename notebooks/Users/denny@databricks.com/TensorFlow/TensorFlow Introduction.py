# Databricks notebook source exported at Thu, 21 Jul 2016 17:30:55 UTC
# MAGIC %md ## TensorFlow Introduction
# MAGIC This is an introduction to TensorFlow using TensorFlow with Databricks and TensorFrames

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
# MAGIC ### TensorFlow Introduction
# MAGIC This set of cells is based on the [TensorFlow Introduction](https://www.tensorflow.org/versions/r0.9/get_started/index.html).  As noted, the first part of this code builds the data flow graph. TensorFlow does not actually run any computation until the session is created and the run function is called (`sess.run(init)`).

# COMMAND ----------

import tensorflow as tf
import numpy as np

# Create 100 phony x, y data points in NumPy, y = x * 0.1 + 0.3
x_data = np.random.rand(100).astype(np.float32)
y_data = x_data * 0.1 + 0.3

# Try to find values for W and b that compute y_data = W * x_data + b
# (We know that W should be 0.1 and b 0.3, but Tensorflow will
# figure that out for us.)
W = tf.Variable(tf.random_uniform([1], -1.0, 1.0))
b = tf.Variable(tf.zeros([1]))
y = W * x_data + b

# Minimize the mean squared errors.
loss = tf.reduce_mean(tf.square(y - y_data))
optimizer = tf.train.GradientDescentOptimizer(0.5)
train = optimizer.minimize(loss)

# Before starting, initialize the variables.  We will 'run' this first.
init = tf.initialize_all_variables()

# Launch the graph.
sess = tf.Session()
sess.run(init)

# Fit the line.
for step in range(201):
    sess.run(train)
    if step % 20 == 0:
        print(step, sess.run(W), sess.run(b))

# Learns best fit is W: [0.1], b: [0.3]