from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col, asc,desc, count, max, min


conf = SparkConf()
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .appName("Python Spark create RDD example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

collectionsEvents = spark.read.option("Header",True).csv("google-traces/collection_events/collection_events-000000000000.csv", inferSchema=True)
instanceEvents = spark.read.option("Header",True).csv("google-traces/instance_events/instance_events-000000000031.csv", inferSchema=True)

##Primeiro irei responder quantos jobs são submetidos por horas no trace
## Nesse caso seria necessário filtrar linhas que são pertencentes a alloc set
## mas como não tenho o campo collection_type, vou considerar que todas as linhas são jobs
jobsSubmitted = collectionsEvents.filter(collectionsEvents.type == 0)
jobsSubmitted = jobsSubmitted.withColumn('time',col('time').cast("int"))
jobsSubmitted = collectionsEvents.filter(collectionsEvents.time > 0)

jobsCount = jobsSubmitted.select('time').rdd.count()
microToHour = 3.6e+9

traceEnd = jobsSubmitted.select('time').rdd.max()[0]
traceStart = jobsSubmitted.select('time').rdd.min()[0]

hoursElapsed = (traceEnd-traceStart)/microToHour

jobsPerHour = jobsCount/hoursElapsed