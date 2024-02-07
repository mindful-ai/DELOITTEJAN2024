#!/usr/bin/env python
# coding: utf-8

# # Collaborative Filtering ALS for a retail data set
# 
# Import findspark and initiate. Then import pyspark

# In[ ]:


import findspark
findspark.init('/usr/local/spark')
import pyspark


# Start Spark Session

# In[ ]:


from pyspark.sql import SparkSession


# In[ ]:


spark = SparkSession.builder.appName("PySpark Collaborative Filtering ALS example").getOrCreate()


# Import libraries for ALS

# In[ ]:


from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import *


# Create Spark Context object

# In[ ]:


retailData = spark.read.load("retail_2013.csv", format="csv", inferSchema="true", header="false")
retailData.count()


# In[ ]:


retailData.printSchema()


# Collaborative Filtering of spark.ml uses ALS (Alternatiing Least Squares) algorithm.
# The input dataframe is expected to have the three columns userCol, itemCol and ratingCol.
# In our retail data set, we will use the customer id column as userCol.
# We will generate a product index for each product name (column _c4) and use them as itemCol.
# We will get the number of times a user bought a product and use it for ratingCol.
# We will set the input parameter implicitPrefs as true because we do not have explicit feedback such as rating of a product.

# In[ ]:


from pyspark.ml.feature import StringIndexer


# In[ ]:


indexer = StringIndexer(inputCol="_c4", outputCol="ProductIndex")
retailData1 = indexer.fit(retailData).transform(retailData)


# In[ ]:


retailData1.show(truncate=False)


# In[ ]:


retailData2 = retailData1.select('ProductIndex',col("_c6").alias("CustomerId"))
retailData2.printSchema()
retailData2.show(5)


# In[ ]:


retailData3=retailData2.groupBy("ProductIndex", "CustomerId").count()


# In[ ]:


retailData3.printSchema()
retailData3.show()


# In[ ]:


# Changing column name of count to Count and type to double


# In[ ]:


retailData4=retailData3.withColumn("Count", expr("CAST(count AS DOUBLE)"))


# In[ ]:


retailData4.printSchema()
retailData4.show()


# Define ALS object and traing the model using fit method on the dataframe

# In[ ]:


als = ALS(maxIter=5, regParam=0.01, userCol="CustomerId", itemCol="ProductIndex", ratingCol="Count", implicitPrefs=True, alpha=1.0)


# In[ ]:


model = als.fit(retailData4)


# In[ ]:


# Create a dataframe, productDF that contains distinct product names and product indexes


# In[ ]:


productDF = retailData1.select(col("_c4").alias("ProductName"),'ProductIndex').distinct()
productDF.printSchema()


# In[ ]:


productDF.count()


# In[ ]:


productDF.orderBy('ProductIndex').show()


# To get recommendations for a customer, for example customer id 43124, create a data frame, testDF from productDF that contains all the product indexes (and product names). To this add a column CustomerId whose value is the given customer id which in this case 43124 and column Count as 0.0.

# In[ ]:


testDF=productDF.withColumn('CustomerId', lit(43124)).withColumn('Count', lit(0.0))
testDF.printSchema()
testDF.show()


# In[ ]:


predictions = model.transform(testDF)
predictions.printSchema()


# In[ ]:


predictions.orderBy("prediction", ascending=False).limit(5).show()


# In[ ]:


testDF2=productDF.withColumn('CustomerId', lit(99970)).withColumn('Count', lit(0.0))
testDF2.printSchema()
testDF2.show()


# In[ ]:


predictions2 = model.transform(testDF2)
predictions2.printSchema()


# In[ ]:


predictions2.orderBy("prediction", ascending=False).limit(5).show()

