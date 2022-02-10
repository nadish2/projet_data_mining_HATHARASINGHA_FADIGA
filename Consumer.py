from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, Row
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession

def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def process(rdd):
    # Get the singleton instance of SparkSession
    spark = getSparkSessionInstance(rdd.context.getConf())
    #rowRdd = rdd.map(lambda v: Row(val=v))
    df=spark.createDataFrame(rdd,schema= ['name','age',"genre","ville"])
    df.printSchema()
    df.write.format('jdbc').options(
            url='jdbc:mysql://192.168.33.14/projet_datamining',
            dbtable='testprojet',
            user='nadeesha',
            password='nadeesha').mode('append').save()


# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "Kafka Stroke")
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)
ssc.checkpoint('.')


# Create a DStream that will connect to hostname:port, like localhost:9999
kds = KafkaUtils.createDirectStream(ssc, ["projet_datamining"], {"metadata.broker.list": "192.168.33.13:9092"})

lines = kds.map(lambda x: x[1])

#Transforme la première ligne (string) en liste de mots
rdd = lines.map(lambda s: s.split(','))

#Il sélectionne les personnes atteintes du cancer du sein (diagnostic=Malin)
#rdd2=rdd.filter(lambda x: x[1] =='M') 


#Affiche les 10 premiers éléments de chaque RDD généré 
rdd.pprint()

rdd.foreachRDD(process)


ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate