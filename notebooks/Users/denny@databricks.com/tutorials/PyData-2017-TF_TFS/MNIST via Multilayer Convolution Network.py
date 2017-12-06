#
# MNIST via Multilayer Convolution Network
#   This script is baed on the [MNIST for Deep Learning Experts using TensorFlow](https://github.com/tensorflow/tensorflow/blob/r1.2/tensorflow/examples/tutorials/mnist/mnist_deep.py)
#


# Setup
# 	- Ensure TensorFlow is installed via PyPi
#   - Note this is a demo script as opposed to a Python script file; 
#		i.e. execute via python shell 


#
# 1. Start Python 
# 	- type "python" within bash terminal
#


# ## Convolution, pooling, weights and biases
# The first method helps to create a 2-D convolutional layer for a specified input ```x``` and specified kernel size ```W```. The ```strides``` parameter specifies the *movement* of the kernel on the original image: in this case each convolution will *skip* one pixel at a time. The ```padding``` parameter keeps the output size of the features ```SAME``` as the input.
# The pooling layers will be created using the ```max_pool(...)``` method. Each of our pooling layers has a 2x2 kernel and will skip---the ```strides``` parameter---2 rows and 2 columns at a time. The ```padding``` parameter here will apply padding if any downsampling results in a fractional number of pixels.
# Every neuron (apart from those in the input layer) is connected to its predecessors via a number of links, each with a specific weight attached to it. The code below initializes the weights to a random number drawn from a ```truncated_normal``` distribution with standard deviation of 0.1. The ```shape``` parameter is used by the method's logic to determine how many weights to create.
# Just like the weights, each neuron has some *tuneable* bias. The ```bias_variable(...)``` method initializes the biases for the neurons to a ```constant``` equal to 0.1; these biases change as we train the network.


#
# 2. Setup functions
#
def conv2d(x, W):
	"""conv2d returns a 2d convolution layer with full stride."""
	return tf.nn.conv2d(x, W, strides=[1, 1, 1, 1], padding='SAME')


def max_pool_2x2(x):
	"""max_pool_2x2 downsamples a feature map by 2X."""
	return tf.nn.max_pool(x, ksize=[1, 2, 2, 1],
	                    strides=[1, 2, 2, 1], padding='SAME')

# Weight Initialization
def weight_variable(shape):
	"""weight_variable generates a weight variable of a given shape."""
	initial = tf.truncated_normal(shape, stddev=0.1)
	return tf.Variable(initial)


def bias_variable(shape):
	"""bias_variable generates a bias variable of a given shape."""
	initial = tf.constant(0.1, shape=shape)
	return tf.Variable(initial)




# ## Create the model
# The ```deepnn(...)``` method builds the artificial neural network we will use to classify the digits. 
# Here's the overview of the structure of the network we will use:
# ![](http://tomdrabas.com/data/PyData/ConvStructure_NoTanh.png)
# 1. The input layer is a grey scale image of 28x28 pixels. This is accomplished with the ```tf.reshape(...)``` method. The first parameter to the function is the *tensor* we want to reshape, and the other one specifies the desired shape of the output.
# 2. Convolution layer 1 maps one grayscale image to 32 feature maps using 5x5 kernels using ReLU (rectifier) activation function
# 3. Pooling layer 1 down samples image by 2x so you have a 14x14 matrix 
# 4. Convolution layer 2 maps 32 feature maps to 64 using ReLU (rectifier) activation function
# 5. Pooling layer 2  down samples by 2x with 64 images of 7x7 (vs. 14x14)
# 6. The fully connected feed-forward part maps the 64 features of 7x7 pixels to an array of 1024 neurons that then get passed throught the ```argmax(...)``` method to come up with an actual output with one activated neuron. The ```matmul(...)``` method is nothing more than a matrix multiplication applied to an input matrix and corresponding matrix of weights.


def deepnn(x):
	"""deepnn builds the graph for a deep net for classifying digits.
	Args:
	x: an input tensor with the dimensions (N_examples, 784), where 784 is the
	number of pixels in a standard MNIST image.
	Returns:
	A tuple (y, keep_prob). y is a tensor of shape (N_examples, 10), with values
	equal to the logits of classifying the digit into one of 10 classes (the
	digits 0-9). keep_prob is a scalar placeholder for the probability of
	dropout.
	"""
	# Reshape to use within a convolutional neural net.
	# Last dimension is for "features" - there is only one here, since images are
	# grayscale -- it would be 3 for an RGB image, 4 for RGBA, etc.
	x_image = tf.reshape(x, [-1, 28, 28, 1])
	#
	# First convolutional layer - maps one grayscale image to 32 feature maps.
	W_conv1 = weight_variable([5, 5, 1, 32])
	b_conv1 = bias_variable([32])
	h_conv1 = tf.nn.relu(conv2d(x_image, W_conv1) + b_conv1)
	#
	# Pooling layer - downsamples by 2X.
	h_pool1 = max_pool_2x2(h_conv1)
	#
	# Second convolutional layer -- maps 32 feature maps to 64.
	W_conv2 = weight_variable([5, 5, 32, 64])
	b_conv2 = bias_variable([64])
	h_conv2 = tf.nn.relu(conv2d(h_pool1, W_conv2) + b_conv2)
	#
	# Second pooling layer.
	h_pool2 = max_pool_2x2(h_conv2)
	#
	# Fully connected layer 1 -- after 2 round of downsampling, our 28x28 image
	# is down to 7x7x64 feature maps -- maps this to 1024 features.
	W_fc1 = weight_variable([7 * 7 * 64, 1024])
	b_fc1 = bias_variable([1024])
	h_pool2_flat = tf.reshape(h_pool2, [-1, 7*7*64])
	h_fc1 = tf.nn.relu(tf.matmul(h_pool2_flat, W_fc1) + b_fc1)
	#
	# Dropout - controls the complexity of the model, prevents co-adaptation of
	# features.
	keep_prob = tf.placeholder(tf.float32)
	h_fc1_drop = tf.nn.dropout(h_fc1, keep_prob)
	#
	# Map the 1024 features to 10 classes, one for each digit
	W_fc2 = weight_variable([1024, 10])
	b_fc2 = bias_variable([10])
	y_conv = tf.matmul(h_fc1_drop, W_fc2) + b_fc2
	return y_conv, keep_prob


# ## Training the model
# This is the main script of the tutorial where we use all that we have defined so far. 
# First, we read in the ```MNIST_data``` and specify that the output is to be ```one_hot``` encoded. Next, we create the placeholders for our input and output. Using the previously defined ```deepnn(...)``` method we create our DNN to be trained. 
# The goal of training our network is to minimze the ```cross_entropy```; the ```cross_entropy``` is defined as an average of the outputs from the ```softmax``` layer. We are using the ```AdamOptimizer``` with a specified learning rate of 0.0001. 
# To determine if the network has produced a correct prediction we use the ```argmax(...)``` method that returns an index associated with the maximum value in our output layer; this is then compared with the *desired* signal using the ```equal(...)``` method. 
# The overall accuracy is calculated as a mean of the correct and incorrect responses.
# To train our network we use batches of 50 images; the weights are adjusted only after each batch iteration. 

# Import TensorFlow
import tensorflow as tf

# Import MNIST digit images data
from tensorflow.examples.tutorials.mnist import input_data
mnist = input_data.read_data_sets("MNIST_data/", one_hot=True)

# Create the model
x = tf.placeholder(tf.float32, [None, 784])

# Define loss and optimizer
y_ = tf.placeholder(tf.float32, [None, 10])

# Build the graph for the deep net
y_conv, keep_prob = deepnn(x)

cross_entropy = tf.reduce_mean(
  tf.nn.softmax_cross_entropy_with_logits(labels=y_, logits=y_conv))
train_step = tf.train.AdamOptimizer(1e-4).minimize(cross_entropy)
correct_prediction = tf.equal(tf.argmax(y_conv, 1), tf.argmax(y_, 1))
accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

# Launch the model
sess = tf.InteractiveSession()

# Initialize the variables
tf.global_variables_initializer().run()

# By default, should have the range go to 20,000 
for i in range(1500):
	batch = mnist.train.next_batch(50)
	if i % 100 == 0:
		train_accuracy = accuracy.eval(feed_dict={x: batch[0], y_: batch[1], keep_prob: 1.0})
		print('step %d, training accuracy %g' % (i, train_accuracy))
	train_step.run(feed_dict={x: batch[0], y_: batch[1], keep_prob: 0.5})  



## Finally, we test the accuracy of our trained network.
print('test accuracy %g' % accuracy.eval(feed_dict={x: mnist.test.images[:500, :784], y_: mnist.test.labels[:500,:10], keep_prob: 1.0}))




