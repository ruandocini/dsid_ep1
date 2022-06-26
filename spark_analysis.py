from statistics import mean
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col, asc,desc, count, max, min, round, avg, stddev
import functools
import argparse


MICRO_TO_HOUR = 3.6e+9
MICRO_TO_DAY = 8.64e+10

def resourceRequestHourly(events,operation='sum',dimension='hour'):

    dimension_options = {
        'hour':MICRO_TO_HOUR,
        'day':MICRO_TO_DAY
    }

    events = events.withColumn('time',col('time').cast("long"))
    events = events.withColumn('resource_request_memory',col('`resource_request.memory`').cast("float"))
    events = events.withColumn('resource_request_cpus',col('`resource_request.cpus`').cast("float"))

    events = events.filter(events.time > 0)
    events = events.withColumn(f'{dimension}OfTrace',round(col('time')/dimension_options[dimension],0))

    if operation == 'sum':
        usagePerHour = events.groupBy(f'{dimension}OfTrace').sum('resource_request_memory','resource_request_cpus')

    if operation =='avg':
        usagePerHour = events.groupBy(f'{dimension}OfTrace').avg('resource_request_memory','resource_request_cpus')

    if operation =='stddev':
        usagePerHour = events.groupBy(f'{dimension}OfTrace').agg(stddev('resource_request_memory'),stddev('resource_request_cpus'))

    usagePerHour = usagePerHour.orderBy(col(f'{dimension}OfTrace').asc())

    return usagePerHour

def jobsTasksHour(events):

    """

        RESPOSTA AO TOPICO 3 e 4

        Primeiro irei responder quantos jobs/tasks são submetidos por horas no trace
        Como os filtros que faria seriam os mesmos, citados abaixo, construi uma função
        agnostica pra qual tipo de evento estou tratando (job ou task)
        Nesse caso seria necessário filtrar linhas que são pertencentes a alloc set
        mas como não tenho o campo collection_type, vou considerar que todas as linhas são jobs
        O 0 do da coluna Type é referente as ações de SUBMIT do trace, nas quais estamos
        interessados nesta função.
        Seleciono os campos de tempo maiores que zero para filtrar coisas que aconteceram
        antes do trace ou dps deles, demonstrados no dataset pelos valores (0 e 2e63-1)

    """

    events = events.withColumn('casted_time',col('time').cast("long"))
    events = events.filter(events.casted_time > 0)
    submitted = events.filter(events.type == 0)

    count = submitted.rdd.count()

    traceEnd = events.select(max('casted_time')).collect()[0][0]
    traceStart = events.select(min('casted_time')).collect()[0][0]

    hoursElapsed = (traceEnd-traceStart)/microToHour

    eventsPerHour = count/hoursElapsed

    return eventsPerHour

def jobsTaskHourbyHour(events):
    microToHour = 3.6e+9

    events = events.withColumn('time',col('time').cast("long"))
    events = events.filter(events.time > 0)
    events = events.filter(events.type == 0)


    events = events.withColumn('hourOfTrace',round(col('time')/microToHour,0))

    countJobsTaskHour = events.groupBy('hourOfTrace').count()

    countJobsTaskHour = countJobsTaskHour.orderBy(col("hourOfTrace").asc())
    
    return countJobsTaskHour

def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)
    
 
parser = argparse.ArgumentParser()
parser.add_argument('--dimension', required=True)
args = parser.parse_args()

conf = SparkConf()
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .appName("Google borg traces evaluation") \
    .getOrCreate()

collectionsEvents = spark.read.option("Header",True).csv("google-traces/collection_events/collection_events-000000000000.csv", inferSchema=True)

instanceFilesIndexes = list(range(0,45))
instanceFilesIndexes = [
    '0'+str(index) if index < 10 else str(index)
    for index in instanceFilesIndexes
]

eventsFrames = [
    spark.read.option("Header",True).csv(f"google-traces/instance_events/instance_events-0000000000{index}.csv", inferSchema=True)
    for index in instanceFilesIndexes
]

unifiedInstanceEvents = unionAll(eventsFrames)

resourceRequestHourly(unifiedInstanceEvents,'avg',args.dimension).write.csv(f'resource_avg_usage_{args.dimension}',header=True)
resourceRequestHourly(unifiedInstanceEvents,'sum',args.dimension).write.csv(f'resource_sum_usage_{args.dimension}',header=True)
# resourceRequestHourly(unifiedInstanceEvents,'stddev').write.csv('resource_stddev_usage_hourly',header=True)

# avgJobHour = jobsTasksHour(collectionsEvents)
# avgTaskHour = jobsTasksHour(unifiedInstanceEvents)
# avgResourceHourly = avgResourceRequestHourly(unifiedInstanceEvents)
# totalResourceHourly = sumResourceRequestHourly(unifiedInstanceEvents)
# jobsTaskHourbyHour(unifiedInstanceEvents)