from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

conf = SparkConf()
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .appName("Python Spark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

distFile = spark.read.option("Header",True).csv("google-traces/collection_events/collection_events-000000000000.csv")

distFile.show()