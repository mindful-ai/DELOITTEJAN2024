#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import findspark
findspark.init('/usr/local/spark')
import pyspark


# Import StreamingContext which is the main entry point for all streaming functionality.

# In[ ]:


from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# Create a SparkContext with two execution threads, and StreamingContext with batch interval of 1 second.

# In[ ]:


sc = SparkContext("local[2]", "Socket Stream - Stock Data Case Study")
ssc = StreamingContext(sc, 1)


# Read and cache previous max price into an RDD.

# In[ ]:


inputRDD = sc.textFile("previous_max_price.csv")
inputRDD.persist()


# In[ ]:


type(inputRDD)


# In[ ]:


inputRDD.count()


# In[ ]:


inputRDD.take(5)


# Each record of the RDD is a line of text with fields delimited by comma. We need to split the text line at the delimiter into individual strings of stock symbol and previosu max price.

# In[ ]:


prevMaxRDD1 = inputRDD.map(lambda line: line.split(','))


# In[ ]:


prevMaxRDD1.count()


# In[ ]:


prevMaxRDD1.take(5)


# In[ ]:


prevMaxRDD1.first()[1]


# Convert the previous max stock price to float type.

# In[ ]:


prevMaxRDD2 = prevMaxRDD1.map(lambda line: (line[0], float(line[1])))


# In[ ]:


prevMaxRDD2.take(5)


# In[ ]:


prevMaxRDD2.first()[1]


# Using spark streaming context, we can create a DStream that represents streaming data from a TCP source, specified as hostname (e.g. localhost) and port (e.g. 9999).

# In[ ]:


inStream = ssc.socketTextStream("localhost", 9999)


# This DStream named 'inStream' represents the stream of data.

# In[ ]:


type(inStream)


# Each record in this DStream is a line of text with fields delimited by comma. We need to split the test line at the delimiter into individual strings.

# In[ ]:


stockStream1 = inStream.map(lambda line: line.split(','))


# In[ ]:


type(stockStream1)


# Convert each record into record of key-value pair wth key being the stock symbol and value being a tuple of time stamp string and current price of float type.

# In[ ]:


stockStream2 = stockStream1.map(lambda line: (line[0], (line[1], float(line[2]))))


# We need to join data stream records with those of the previous max RDD on stock symbol field.
# We can use the join function, but since it is an RDD-to-RDD function we need to use "transform" method on the stream so that we can apply it.
# https://spark.apache.org/docs/2.1.1/streaming-programming-guide.html#transformations-on-dstreams

# In[ ]:


joinedRecords = stockStream2.transform(lambda recs: recs.join(prevMaxRDD2))


# Joined records will be of the form key-value: stock_symbol, ((timestamp,current price),previous max price).
# So, record[0] will be stock_symbol, record[1] will have 2 fields.
# And record[1] will have 2 parts - One is record[1][0] which has (time spamp, current price); the other is record[1][1] which has previous max price.
# 
# We need to filter for this records in which current price is greater than previous max price.

# In[ ]:


filteredRecords = joinedRecords.filter(lambda recs: recs[1][0][1]>=recs[1][1])


# Use pprint (pretty print) function to print record matching the filter

# In[ ]:


filteredRecords.pprint()


# When the above code is executed the computations are only set up by SparkStreaming. They will be executed when SparkStreaming is started as below.

# In[ ]:


ssc.start()


# We can terminate it by interrupting the kernel (sending Control+C)

# In[ ]:


ssc.awaitTermination()


# In[ ]:




