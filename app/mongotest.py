import numpy as np
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import DateType

def get_weather_data(spark):
    pipeline1 = "{'$project': {'day': 1,'month':1,'year':1,'sha_tin_max':1,'sha_tin_min':1,'_id':0}}"
    pipeline2 = "{'$project': {'day': 1,'month':1,'year':1,'happy_velley_max':1,'happy_velley_min':1,'_id':0}}"
    df1 = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
        .option("database","HorseRacing")\
            .option("collection","WeatherData").option('pipeline',pipeline1).load()

    df2 = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
        .option("database","HorseRacing")\
            .option("collection","WeatherData").option('pipeline',pipeline2).load()
    # raw = spark.read.csv('weather_data.csv',header=True, inferSchema=True)

    
    # df1 = raw.select('day','month','year','sha_tin_max','sha_tin_min','_c0')
    # df2 = raw.select('day','month','year',"happy_velley_max",'happy_velley_min','_c0')
    
    
    df1 = df1.select('*',lit('Sha Tin').alias('Weather_Course'))\
             .withColumnRenamed('sha_tin_max','MaxTemp')\
             .withColumnRenamed('sha_tin_min','MinTemp')\
             .withColumnRenamed('_c0','_id') 
    
    df2 = df2.select('*',lit('Happy Valley').alias('Weather_Course'))\
             .withColumnRenamed('happy_velley_max','MaxTemp')\
             .withColumnRenamed('happy_velley_min','MinTemp')\
             .withColumnRenamed('_c0','_id')
    
    df1 = df1.select(concat_ws('/',df1.month.cast(IntegerType()),df1.day.cast(IntegerType()),df1.year.cast(IntegerType())).alias('Weather_Date'),'Weather_Course','MaxTemp','MinTemp')
    df2 = df2.select(concat_ws('/',df2.month.cast(IntegerType()),df2.day.cast(IntegerType()),df2.year.cast(IntegerType())).alias('Weather_Date'),'Weather_Course','MaxTemp','MinTemp')
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    df1 = df1.withColumn('Weather_Date',to_date(col('Weather_Date'),"M/dd/yyyy"))
    df2 = df2.withColumn('Weather_Date',to_date(col('Weather_Date'),"M/dd/yyyy"))
    return df1.union(df2)

if __name__ == "__main__":
    #initialize spark context
    spark = SparkSession.builder.master('spark://vm1:7077').appName("Data Processing_Horse Racing").config("spark.mongodb.input.uri", "mongodb://20.187.94.145/HorseRacing.WeatherData") \
    .config("spark.mongodb.output.uri", "mongodb://20.187.94.145/HorseRacing.WeatherData")\
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1').getOrCreate()

    df_weather = get_weather_data(spark)

    print(df_weather.show())

    spark.stop()