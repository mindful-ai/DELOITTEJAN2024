# This application needs to be run as:
# spark-submit --master local[2] --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 streamingkafka1.py

# Import StreamingContext which is the main entry point for all streaming functionality.

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from pyspark.streaming.kafka import KafkaUtils

# Create a SparkContext with two execution threads, and StreamingContext with batch interval of 1 second.

sc = SparkContext("local[2]", "StreamingKafka1")
ssc = StreamingContext(sc, 1)

# Create an input DStream using KafkaUtils.createStream passing the parameters – Spark Streaming Context, Zookeeper connection port, consumer group name and topic name with number of partitions.

kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'kafka-spark-streaming', {'kafka-streaming1':1})

# This is termed a Receiver-based approach as a Receiver is created by the above API call. The data received from Kafka through the Receiver is stored in Spark executors and is processed by the job launched by Spark Streaming.
# Data is handled as a normal RDD to perform word count.

lines = kafkaStream.map(lambda x: x[1])

counts = lines.flatMap(lambda line: line.split(' ')).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

counts.pprint()

ssc.start()

# We can terminate it by interrupting the kernel (sending Control+C)

ssc.awaitTermination()
