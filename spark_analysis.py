from statistics import mean
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col, asc,desc, count, max, min, round, avg, stddev
from pyspark.sql.functions import udf,col
import functools
import argparse

MICRO_TO_HOUR = 3.6e+9
MICRO_TO_DAY = 8.64e+10
MICRO_TO_SECONDS = 1e-6

def resourceRequest(events,operation='sum',dimension='hour'):

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

def jobTypeFrequency(events,dimension='hour'):

    dimension_options = {
        'hour':MICRO_TO_HOUR,
        'day':MICRO_TO_DAY
    }

    events = events.withColumn('casted_time',col('time').cast("long"))
    events = events.filter(events.casted_time > 0)

    events = events.withColumn(f'{dimension}OfTrace',round(col('time')/dimension_options[dimension],0))

    frequency = events.groupBy(f'{dimension}OfTrace','type').count()

    frequency = frequency.orderBy(col(f'{dimension}OfTrace').asc())

    return frequency

def jobsTasksOnAverageByDimension(events,dimension='hour'):

    dimension_options = {
        'hour':MICRO_TO_HOUR,
        'day':MICRO_TO_DAY
    }

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

    timeElapsed = (traceEnd-traceStart)/dimension_options[dimension]

    eventsPerDimension = count/timeElapsed

    return eventsPerDimension

def jobsTaskAlongDimension(events,dimension='hour'):

    dimension_options = {
        'hour':MICRO_TO_HOUR,
        'day':MICRO_TO_DAY
    }

    events = events.withColumn('time',col('time').cast("long"))
    events = events.filter(events.time > 0)
    events = events.filter(events.type == 0)


    events = events.withColumn(f'{dimension}OfTrace',round(col('time')/dimension_options[dimension],0))

    countJobsTaskHour = events.groupBy(f'{dimension}OfTrace').count()

    countJobsTaskHour = countJobsTaskHour.orderBy(col(f'{dimension}OfTrace').asc())
    
    return countJobsTaskHour

def timeJobUntilTask(collectionsEvents, unifiedInstanceEvents):

    dimension_options = {
        'seconds':MICRO_TO_SECONDS,
        'hour':MICRO_TO_HOUR,
        'day':MICRO_TO_DAY
    }

    collectionsEvents = collectionsEvents.withColumn('job_time',col('time').cast("long"))
    collectionsEvents = collectionsEvents.withColumn('col_id',col('collection_id'))
    collectionsEvents = collectionsEvents.withColumn('col_type',col('type'))
    collectionsEvents = collectionsEvents.filter(collectionsEvents.job_time > 0)

    unifiedInstanceEvents = unifiedInstanceEvents.withColumn('task_time',col('time').cast("long"))
    unifiedInstanceEvents = unifiedInstanceEvents.filter(unifiedInstanceEvents.task_time > 0)

    unifiedInstanceEvents = unifiedInstanceEvents.groupBy('collection_id','type').min('task_time')
    collectionsEvents = collectionsEvents.groupBy('col_id','col_type').min('job_time')

    joinedSets = collectionsEvents.join(
        unifiedInstanceEvents,
        on=[collectionsEvents.col_id == unifiedInstanceEvents.collection_id, collectionsEvents.col_type == unifiedInstanceEvents.type],
        how="inner"
    )
    
    joinedSets = joinedSets.withColumn('timeGapJobTask',((col('min(task_time)') - col('min(job_time)'))))

    joinedSets = joinedSets.groupBy('timeGapJobTask').count()

    joinedSets = joinedSets.orderBy(col('count').desc())

    return joinedSets

def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)
    
def jobTier(priority):
    if priority <= 99:
        return 'Free Tier'
    if priority <= 115:
        return 'Best-effort Batch'
    if priority <= 119:
        return 'Mid-tier'
    if priority <= 359:
        return 'Production tier'
    if priority >= 360:
        return 'Monitoring tier'
 
def eventTypePerTier(trace,event=0):
    udf_tier = udf(lambda x:jobTier(x),StringType())
    trace = trace.withColumn('jobTier',udf_tier(col('priority')))
    submitted = trace.filter(trace.type == event)
    submitted = submitted.groupBy('jobTier').count()
    return submitted

parser = argparse.ArgumentParser()
parser.add_argument('--dimension', required=True)
parser.add_argument('--task', required=True)
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

if args.task == '1':
    # ANALISE 1
    resourceRequest(unifiedInstanceEvents,'avg',args.dimension).write.csv(f'resource_avg_usage_{args.dimension}',header=True)
    resourceRequest(unifiedInstanceEvents,'sum',args.dimension).write.csv(f'resource_sum_usage_{args.dimension}',header=True)
    resourceRequest(unifiedInstanceEvents,'stddev',args.dimension).write.csv(f'resource_stddev_usage_{args.dimension}',header=True)

if args.task == '2':
    # ANALISE 2
    jobPerTier = eventTypePerTier(collectionsEvents,0)
    jobPerTier.write.csv(f'jobsSubmittedPerTier',header=True)
    jobPerTier = eventTypePerTier(collectionsEvents,5)
    jobPerTier.write.csv(f'jobsFailedPerTier',header=True)

if args.task == '3':
    #ANALISE 3
    avgJob = jobsTasksOnAverageByDimension(collectionsEvents,dimension=args.dimension)
    print(f"Jobs by {args.dimension}: " + str(avgJob))
    jobsTaskAlongDimension(collectionsEvents,dimension=args.dimension).write.csv(f'jobs_{args.dimension}',header=True)

if args.task == '4':
    #ANALISE 4
    avgTask = jobsTasksOnAverageByDimension(unifiedInstanceEvents,dimension=args.dimension)
    print(f"Tasks by {args.dimension}: " + str(avgTask))
    jobsTaskAlongDimension(unifiedInstanceEvents,dimension=args.dimension).write.csv(f'tasks_{args.dimension}',header=True)

if args.task == '5':
    #ANALISE 5
    timeUntilStart = timeJobUntilTask(collectionsEvents,unifiedInstanceEvents)
    timeUntilStart.write.csv(f'time_until_start',header=True)

if args.task == '6':
    #ANALISE 6
    jobTypeFrequency(collectionsEvents,args.dimension).write.csv(f'jobs_type_frequency_{args.dimension}',header=True)