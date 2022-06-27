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
    """

    Função construída para mostrar a quantidade de recurso requerido por 
    uma certa dimensão de tempo, por exemplo, como variou a requisição
    de CPU durante as horas que temos disponíveis no trace.

    Os cast's para long são necessários por causa do typing do spark, que estava
    tendo dificuldade na leitura dos arquivos.

    Selecionamos apenas valores maiores que 0 no tempo, pois valores menores que esse
    representam que a ação aconteceu fora do trace, segundo o pdf disponibilizado
    sobre o conjunto de dados, podendo ser 0 ou 2e63-1, quando esse ocorre após o termino
    do trace.

    A conversão do timestamp é feita para a hora/dia que o evento ocorreu, para podermos 
    observar numa granularidade que faça mais sentido.

    E após isso podemos escolher uma função de agregação para aplicar nos dados.

    Sendo  assim no retorno temos a agregação dos recursos de CPU e de memoria, pelo
    período de tempo desejado.

    Essa função foi criado com intenção de resolver a analise 1:
    Como é a requisição de recursos computacionais (memória e CPU) do cluster durante esse tempo? 

    """

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

    """

    Criada para mostrar qual a distribuição dos eventos (FAILED, SUBMITTED, STOPPED) dentro
    dos dados que temos do cluster, tambem funciona com o parametro de dimensão, ou seja,
    vemos essa variação ao longo do tempo.

    Temos o mesmo casting para long da função anterior, o mesmo filtro para logs com tempo valido
    e conversão para a dimensão desejada.

    O que fica como exclusividade dessa função é a implementação da contagem de eventos por dia 
    e por evento, e após isso uma ordenação em ordem crescente de tempo, inicio para o fim do trace.

    Tendo assim como retorno, a quantidade de cada um dos eventos, por dimensão do conjunto de dados

    Essa função foi criada para resolver a análise de número 6:
    Distribuição dos tipos de eventos nos Jobs (coluna type) (extra)

    """

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

    """
    Criada para mostrar a quantidade de jobs ou task submetidos numa determinada
    granularidade de tempo, ou seja, quantos eventos foram em média por hora ou por 
    dia.

    Temos o mesmo cast de long, filtro para retirar valores de tempo não pertencentes 
    ao trace.

    Após isso fazemos um filtro na coluna type, para pegarmos apenas os eventos de
    submissão que são os que nos interessam nesse momento.

    Uma vez o filtro feito, temos que ver o horario de inicio e fim das submissões,
    realizar a diferença e colocar na granularidade correta.

    Um contagem de todas as submissões é feita, e então temos a divisão,
    da contagem pelo período, assim obtendo a média de tasks ou jobs
    submetidas, dependendo do input que for dado a função.

    Essa função gera parte da análise 3 e 4, retornando um float,
    que significa a média citada acima, impressa no console posteriormente.

    """

    dimension_options = {
        'hour':MICRO_TO_HOUR,
        'day':MICRO_TO_DAY
    }

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

    """

    Criada para mostrar como se comportam as submissões ao longo do tempo, ou seja,
    quantas submissões tivemos por período de tempo.
    EX: submissões de task's na hora 1, submissões de jobs na hora 2

    Também parametrizavel pela dimensão (dia e hora)

    Mesmos fitros de tempo valido, e de tipo para pegar apenas submissão, como
    tambbém a conversão de micro segundos.

    Depois temos um agrupamento por dimensão no trace, realizando assim a contagem,
    e uma ordenação em ordem ascendente

    A função retorna a quantidade de jobs/traces ao longo das horas/dias,
    dependendo do input dado a ela.

    Esta função gera a segunda parte da resposta 3 e 4, criando um historico
    de contagem ao londo do tempo do trace.

    """

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

    """
    Função que é responsavel por extrair quanto tempo a primeira task de um job
    demora para ser inicializada.

    Primeiramente temos alguns castings e replicação de colunas para uso no join,
    como as colunas ficam ambiguas é importante ter as colunas nomeadas de maneira
    correta para exercer manipulação futura. Após isso o filtro de tempo valido
    também é efetuado aqui.

    Depois o minimo do tempo de uma task, por id de collection e type, pois,
    como podem existir varios, é importante pegar o primeiro que aparece no trace.

    Fazemos o mesmo para o collection id do job e type, para gerarmos a tabela
    para o join, somente com os dados que serão relevantes para resolver esse problema em si.

    Após isso temos a união das tabelas por collection_id e type.

    E a diferença entre o tempo do job e da primeira task, tendo assim o gap
    entre essas duas ações.

    Depois disso agrupamos esse número, para ter a distribuição de ocorrência dos gaps,
    ou seja, qual o tempo mais comum de espera para primeira task de um job acontecer? Qual é essa distribuição?

    Retornando assim esse dataset em ordem crescente

    Essa parte do código responde a analise 5.
    """
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

    """
    Usada apenas para unir todos os dataset para todas as analises que requerem olhar para os eventos de instancia,
    usando reduce e lambda functions junto com uma função do proprio spark dataset
    """

    return functools.reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)
    
def jobTier(priority):
    """
    Usada para criar uma UDF function do Spark para definir em qual tier um job/task se encaixa.
    É importante definir como um UDF pois UDF's são paralelizaveis, diferentemente se criasemos
    somente uma parte do codigo que não interage com a estrutura do Spark.
    """

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
    
    """
    Tem como intenção observar quantos eventos ocorrem em determinado tier, por exemplo, quantas maquinas falham por Tier?

    Primeiro geramos a UDF da função localizada acima, criada para transformar prioridade em tier e facilitar as analises.
    Apos isso temos um filtro de evento, ou seja, podemos extrair métricas para qualquer evento passado como parametro 
    (SUBMIT = 0, KILL = 7, FINISH = 6).

    E por fim temos um agrupamento pelo tier e a realização de uma contagem, podendo assim
    retornar qualquer coisa que relacione eventos e tiers

    Essa função foi criada para resolver a análise de número 2 

    """

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

## Essa parte é responsavel por gerar os nomes de arquivos
## e ler todos eles, para que seja possível fazer a analise
## num dataset unificado, a lista de 0 á 45, é porque esses são
## os números dos arquivos que possuímos
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