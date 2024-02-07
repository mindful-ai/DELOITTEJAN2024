#!/usr/bin/env python
# coding: utf-8

# ### Network Intrusion Detection - MultiLabel DecisionTree

# Import and initialize findspark followed by importing pyspark 

# In[ ]:


import findspark
findspark.init('/usr/local/spark')
import pyspark


# Create SparkContext in the variable sc

# In[ ]:


sc = pyspark.SparkContext(appName='networkIntrusionDecisionTreeMultiLabel')


# Import the required libraries

# In[ ]:


from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils

from pyspark.mllib.regression import LabeledPoint
from numpy import array

from pyspark.mllib.evaluation import MulticlassMetrics

from pyspark.mllib.linalg import DenseVector


# Read the input data file and create the RDD

# In[ ]:


networkData = sc.textFile("kddcup.data_10_percent")
networkData.count()


# Get the total number of fields from line of the input data which is the comma separated.

# In[ ]:


numfields = len(networkData.take(1)[0].split(","))
print(numfields)


# Get the distinct values of last field from the input lines.
# Create a dictionary assigning a number with the help of zipWithIndex, so the key will be the string from the last field and the value will be the unique number assigned to each string.

# In[ ]:


labels = networkData.map(lambda line: (line.split(",")[numfields-1])).distinct()
labels.count()


# In[ ]:


for x in labels.collect():
    print(x)


# In[ ]:


labelDict = dict(labels.zipWithIndex().collect())


# In[ ]:


for k, v in labelDict.items():
    print(k, v)


# Define a function to create a LabeledPoint from a comma delimited line

# In[ ]:


def get_cleanline(pstr):
    line = pstr.split(",")
    data = [line[0]]+line[4:numfields-1]
    return LabeledPoint(labelDict[line[numfields-1]], array([float(x) for x in data]))


# Run it as a lambda function on the input data. This will create an RDD in which each element is a LabeledPoint.
# And display the label and the features which is a vector.

# In[ ]:


cleandata=networkData.map(lambda line: get_cleanline(line))


# In[ ]:


for line in cleandata.take(10):
    print(line)


# In[ ]:


cleandata.first().label


# In[ ]:


cleandata.first().features


# Split this RDD into training and testing portions and cache them

# In[ ]:


(trainingData, testData) = cleandata.randomSplit([0.9, 0.1])


# In[ ]:


trainingData.cache()
testData.cache()


# Build a multi class label Decisiontree model with the training data. Used gini as the impurity parameter. 

# In[ ]:


model = DecisionTree.trainClassifier(trainingData, numClasses=labels.count(), categoricalFeaturesInfo={}, impurity='gini', maxDepth=4, maxBins=100)


# Run the model on test data using the predict method 

# In[ ]:


predictions = model.predict(testData.map(lambda x: x.features))


# Get the predicted values along with the the labels

# In[ ]:


labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)


# In[ ]:


for line in labelsAndPredictions.take(10):
    print(line)


# Get the metrics from the predicted values

# In[ ]:


metrics = MulticlassMetrics(labelsAndPredictions)


# In[ ]:


metrics.confusionMatrix()


# In[ ]:


metrics.accuracy


# Display the decision tree

# In[ ]:


print('Learned classification tree model:')
print(model.toDebugString())


# Build another model this time with entropy as the impurity parameter. And perform the same tasks as above.

# In[ ]:


model2 = DecisionTree.trainClassifier(trainingData, numClasses=labels.count(), categoricalFeaturesInfo={}, impurity='entropy', maxDepth=4, maxBins=100)


# In[ ]:


predictions2 = model2.predict(testData.map(lambda x: x.features))


# In[ ]:


labelsAndPredictions2 = testData.map(lambda lp: lp.label).zip(predictions2)


# In[ ]:


for line in labelsAndPredictions2.take(10):
    print(line)


# In[ ]:


metrics2 = MulticlassMetrics(labelsAndPredictions2)


# In[ ]:


metrics2.confusionMatrix()


# In[ ]:


metrics2.accuracy


# In[ ]:


print('Learned classification tree model2:')
print(model2.toDebugString())


# Using the model to predict for new set of data points.
# 
# * Write functions to load a file, parse and classify new data using the predict method of the given model.
# * Create a new dict by reversing the key, values of label dictionary so that the names of the predicted values can be displayed instead of only the corresponding the number.

# In[ ]:


def get_currentline(pstr):
    cline = pstr.split(",")
    return [cline[0]]+cline[4:numfields-2]


# In[ ]:


def classify(filename, pmodel):
    currentData = sc.textFile(filename)
    fields = currentData.map(lambda gline: get_currentline(gline))
    return pmodel.predict(fields.map(lambda x: x))


# In[ ]:


labelDictRev = dict((v,k) for k, v in labelDict.items())


# In[ ]:


# Alternate syntax to reverse key, values of a dictionary
# labelDictRev2 = {v: k for k, v in labelDict.items()}


# In[ ]:


for k, v in labelDictRev.items():
    print(k, v)


# Classify new data given in current.txt file using the model with the help of above functions.

# In[ ]:


cLabelsAndPredictions = classify("current.txt", model)


# In[ ]:


for pred in cLabelsAndPredictions.collect():
    print(pred, labelDictRev[int(pred)])


# We can pass model2 also as the parameter to classify function and do the predictions if we find the accuracy of model2 to be better.
