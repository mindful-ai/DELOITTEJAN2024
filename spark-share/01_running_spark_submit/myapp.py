# A Spark application in Python
# It can be run with spark-submit command as follows:
# spark-submit --master local[*]  myapp.py
# spark-submit --master spark://ubuntu:7077 myapp.py
# spark-submit --master yarn myapp.py

import pyspark
sc = pyspark.SparkContext(appName='myApp')

# The above two lines are required to set the SparkContext

lines = sc.textFile("war_and_peace.txt",2)
nonNullLines = lines.filter(lambda line: len(line)>0) 
words = nonNullLines.flatMap(lambda line: line.split())
upperWords = words.map(lambda word: word.upper())
pairedOnes = upperWords.map(lambda uw: (uw, 1))
wordCounts = pairedOnes.reduceByKey(lambda prev, next: prev + next)

for word in wordCounts.take(5):
    print("*****", word)

sc.stop()

