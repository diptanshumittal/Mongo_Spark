import time
import numpy as np
from statistics import mean 
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
import pyspark.sql.functions as f 

def monthSeries():
   series = [[i] for i in range(1,13)]
   return series

def q1_a(df):
   return df.filter((col("eventname") =='opened')).groupBy(to_date(col("event_time")).alias("date")).agg(countDistinct(col("sno")).alias("Event_count")).orderBy("date")

def q1_b(df):
   return df.filter((col("eventname") =='discussed')).groupBy(to_date(col("event_time")).alias("date")).agg(countDistinct(col("sno")).alias("Event_count")).orderBy("date")

def q2(df):
   w = Window.partitionBy('month')
   df2 = df.filter((col("eventname") =='discussed')).groupBy(month(col("event_time")).alias('month') , col("username")).agg(count(col("sno")).alias("Event_count")).orderBy("month", "username")
   return df2.withColumn('maxCount' , f.max('Event_count').over(w)).where(f.col("Event_count") == f.col("maxCount")).drop("maxCount").orderBy(f.col("month"))
  
def q3(df):
   my_spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
   df2 = df.selectExpr('*','date_format(event_time, "u") as weekday')
   df2 = df2.withColumn("weekday", df2["weekday"].cast(IntegerType()) % 7)
   df = df2.selectExpr('*', 'date_sub(to_date(event_time),weekday) as start_of_week')
   cnts = df.filter(df.eventname=="discussed").groupBy(df.start_of_week,df.username).agg(count("*").alias("cnt")).alias("cnts")
   maxs = cnts.groupBy(cnts.start_of_week).agg(max("cnt").alias("mx")).alias("maxs")
   return cnts.join(maxs, (col("cnt") == col("mx")) & (col("cnts.start_of_week") == col("maxs.start_of_week"))).select(col("cnts.start_of_week"), col("cnts.cnt"),col("cnts.username")).orderBy(col('cnts.start_of_week'))

def q4(df):
   my_spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
   df2 = df.selectExpr('*','date_format(event_time, "u") as weekday')
   df2 = df2.withColumn("weekday", df2["weekday"].cast(IntegerType()) % 7)
   df = df2.selectExpr('*', 'date_sub(to_date(event_time),weekday) as start_of_week')
   return df.filter(df.eventname=="opened").groupBy(df.start_of_week).agg(count("*").alias("cnt")).orderBy(col('start_of_week'))
   
def q5(df):
   schema = StructType([StructField("month" , IntegerType())])
   df4 = my_spark.createDataFrame(monthSeries(),schema=schema)
   df3 = df.filter((df.eventname =='merged') & (year(df.event_time)==2010)).cube(month(col("event_time")).alias('month')).count().orderBy('month').filter(col('month').isNull() == 'false')
   return df4.join(df3 , on=['month'] , how = 'left').na.fill(0).orderBy(col('month'))
 
def q6(df):
   return df.cube(to_date(col("event_time")).alias('date')).count().orderBy('date').filter(col('date').isNull() == 'false')

def q7(df):
   df2 = df.filter((df.eventname =='opened') & (year(df.event_time)==2011))
   return df2.cube("username").count().orderBy(desc('count')).filter(col('username').isNull() == 'false')
   
cou = 10
input_uri = "mongodb://127.0.0.1/tyrdatabase1.try1"
output_uri = "mongodb://127.0.0.1/tyrdatabase1.try1"




my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "1")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
print("\n\n\n-----------\nQuery q1_a   :")
start_time = time.time()
for i in range(cou):
   q1_a(df)
print("1 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "2")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
start_time = time.time()
for i in range(cou):
   q1_a(df)
print("2 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
print("Query result  - ")
q1_a(df).show()



my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "1")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
print("\n\n\n-----------\nQuery q1_b   :")
start_time = time.time()
for i in range(cou):
   q1_b(df)
print("1 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "2")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
start_time = time.time()
for i in range(cou):
   q1_b(df)
print("2 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
print("Query result  - ")
q1_b(df).show()



my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "1")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
print("\n\n\n-----------\nQuery q2   :")
start_time = time.time()
for i in range(cou):
   q2(df)
print("1 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "2")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
start_time = time.time()
for i in range(cou):
   q2(df)
print("2 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
print("Query result  - ")
q2(df).show()



my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "1")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
print("\n\n\n-----------\nQuery q3   :")
start_time = time.time()
for i in range(cou):
   q3(df)
print("1 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "2")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
start_time = time.time()
for i in range(cou):
   q3(df)
print("2 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
print("Query result  - ")
q3(df).show()



my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "1")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
print("\n\n\n-----------\nQuery q4   :")
start_time = time.time()
for i in range(cou):
   q4(df)
print("1 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "2")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
start_time = time.time()
for i in range(cou):
   q4(df)
print("2 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
print("Query result  - ")
q4(df).show()



my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "1")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
print("\n\n\n-----------\nQuery q5   :")
start_time = time.time()
for i in range(cou):
   q5(df)
print("1 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "2")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
start_time = time.time()
for i in range(cou):
   q5(df)
print("2 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
print("Query result  - ")
q5(df).show()



my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "1")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
print("\n\n\n-----------\nQuery q6   :")
start_time = time.time()
for i in range(cou):
   q6(df)
print("1 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "2")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
start_time = time.time()
for i in range(cou):
   q6(df)
print("2 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
print("Query result  - ")
q6(df).show()



my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "1")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
print("\n\n\n-----------\nQuery q7   :")
start_time = time.time()
for i in range(cou):
   q7(df)
print("1 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
my_spark = SparkSession\
    .builder\
    .appName("MyApp")\
    .config("spark.mongodb.input.uri", input_uri)\
    .config("spark.mongodb.output.uri", output_uri)\
    .config("spark.executor.instances", "2")\
    .config('spark.jars.packages','org.mongodb.spark:mongo-spark-connector_2.12:2.4.2')\
    .getOrCreate()
df = my_spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
df.createOrReplaceTempView('mytable')
start_time = time.time()
for i in range(cou):
   q7(df)
print("2 executor execution time  :",float(time.time() - start_time)/cou,"seconds  (Averaged over",cou,"runs)")
print("Query result  - ")
q7(df).show(1)