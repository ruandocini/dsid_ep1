from statistics import mean
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col, asc,desc, count, max, min, round
import functools

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
    microToHour = 3600000000.0

    traceEnd = events.select(max('casted_time')).collect()[0][0]
    traceStart = events.select(min('casted_time')).collect()[0][0]

    hoursElapsed = (traceEnd-traceStart)/microToHour

    eventsPerHour = count/hoursElapsed

    return eventsPerHour

def avgResourceRequestHourly(events):
    microToHour = 3.6e+9

    events = events.withColumn('time',col('time').cast("long"))
    events = events.filter(events.time > 0)

    events = events.withColumn('hourOfTrace',round(col('time')/microToHour,0))

    averageCpuUsagePerHour = events.groupBy('hourOfTrace').avg('`resource_request.memory`','`resource_request.cpus`')

    return averageCpuUsagePerHour

def totalResourceRequestHourly(events):
    microToHour = 3.6e+9

    events = events.withColumn('time',col('time').cast("long"))
    events = events.filter(events.time > 0)

    events = events.withColumn('hourOfTrace',round(col('time')/microToHour,0))

    sumCpuUsagePerHour = events.groupBy('hourOfTrace').sum('`resource_request.memory`','`resource_request.cpus`')
    
    return sumCpuUsagePerHour

def jobsTaskHourbyHour(events):
    microToHour = 3.6e+9

    events = events.withColumn('time',col('time').cast("long"))
    events = events.filter(events.time > 0)
    events = events.filter(events.type == 0)


    events = events.withColumn('hourOfTrace',round(col('time')/microToHour,0))

    countJobsTaskHour = events.groupBy('hourOfTrace').count()
    
    return countJobsTaskHour

def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)
    
 
conf = SparkConf()
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .appName("Google borg traces evaluation") \
    .getOrCreate()

collectionsEvents = spark.read.option("Header",True).csv("google-traces/collection_events/collection_events-000000000000.csv", inferSchema=True)

instanceFilesIndexes = list(range(0,1))
instanceFilesIndexes = [
    '0'+str(index) if index < 10 else str(index)
    for index in instanceFilesIndexes
]

eventsFrames = [
    spark.read.option("Header",True).csv(f"google-traces/instance_events/instance_events-0000000000{index}.csv", inferSchema=True)
    for index in instanceFilesIndexes
]

unifiedInstanceEvents = unionAll(eventsFrames)

# avgJobHour = jobsTasksHour(collectionsEvents)
# avgTaskHour = jobsTasksHour(unifiedInstanceEvents)
# avgResourceHourly = avgResourceRequestHourly(unifiedInstanceEvents)
# totalResourceHourly = sumResourceRequestHourly(unifiedInstanceEvents)
# jobsTaskHourbyHour(unifiedInstanceEvents)
# jobsTaskHourbyHour(collectionsEvents).plot.hist()
