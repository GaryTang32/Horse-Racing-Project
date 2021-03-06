import numpy as np
import os
import pyspark.sql.functions as f
import re
import sys
from datetime import date
from flask import Flask
from flask import Flask, render_template
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import MinMaxScaler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import DateType
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
from pyspark.streaming import StreamingContext


# import data
def read_data(spark):
    directory_path1 = os.path.join(os.getcwd(), "Full_Data_Pack_1")
    directory_path2 = os.path.join(os.getcwd(), "Full_Data_Pack_2")

    df_horse = spark.read.csv(r'hdfs://vm1:9000/user/azureuser/horses.csv', header=True, inferSchema=True)
    df_jockey = spark.read.csv(r'hdfs://vm1:9000/user/azureuser/jockeys.csv', header=True, inferSchema=True)
    df_races_sectional = spark.read.csv(r'hdfs://vm1:9000/user/azureuser/races_sectional.csv', header=True, inferSchema=True)
    df_trainer = spark.read.csv(r'hdfs://vm1:9000/user/azureuser/trainer.csv', header=True, inferSchema=True)
    df_sectional = spark.read.csv(r'hdfs://vm1:9000/user/azureuser/sectional_table.csv', header=True, inferSchema=True)
    df_records = spark.read.csv(r'hdfs://vm1:9000/user/azureuser/records.csv', header=True, inferSchema=True)
    df_races = spark.read.csv(r'hdfs://vm1:9000/user/azureuser/races.csv', header=True, inferSchema=True)
    df_foal = spark.read.csv(r'hdfs://vm1:9000/user/azureuser/foal_date_28112021.csv', header=True, inferSchema=True)

    return df_races, df_races_sectional, df_trainer, df_jockey, df_records, df_horse, df_sectional, df_foal


# trainer preprocessing
def trainer_preprocessing(df):
    df = df.withColumn('Total_wins', when(df["Total_wins"].isNull(), 0). \
                       otherwise(df["Total_wins"]))
    df = df.withColumn('Total_second_places', when(df["Total_second_places"].isNull(), 0). \
                       otherwise(df["Total_second_places"]))
    df = df.withColumn('Total_third_places', when(df["Total_third_places"].isNull(), 0). \
                       otherwise(df["Total_third_places"]))
    df = df.select("Trainer_ID", "Total_wins", "Total_second_places", "Total_third_places", "Total_rides")
    df = df.select("Trainer_ID", (col("Total_wins") / col("Total_rides")).alias("trainer_first_place_ratio"), \
                   (col("Total_second_places") / col("Total_rides")).alias("trainer_second_place_ratio"), \
                   (col("Total_third_places") / col("Total_rides")).alias("trainer_third_place_ratio"), \
                   ((col("Total_wins") + col("Total_second_places") + col("Total_third_places")) / col("Total_rides")).alias("trainer_place_ratio"), \
                   ((col("Total_rides") - (col("Total_wins") + col("Total_second_places") + col("Total_third_places"))) / col("Total_rides")).alias("trainer_lose_ratio"))
    return df


# jockey preprocessing
def jockey_preprocessing(df):
    df = df.withColumn('Total_wins', when(df["Total_wins"].isNull(), 0). \
                       otherwise(df["Total_wins"]))
    df = df.withColumn('Total_second_places', when(df["Total_second_places"].isNull(), 0). \
                       otherwise(df["Total_second_places"]))
    df = df.withColumn('Total_third_places', when(df["Total_third_places"].isNull(), 0). \
                       otherwise(df["Total_third_places"]))
    df = df.select("Jockey_ID", "Total_wins", "Total_second_places", "Total_third_places", "Total_rides")
    df = df.select("Jockey_ID", (col("Total_wins") / col("Total_rides")).alias("jockey_first_place_ratio"), \
                   (col("Total_second_places") / col("Total_rides")).alias("jockey_second_place_ratio"), \
                   (col("Total_third_places") / col("Total_rides")).alias("jockey_third_place_ratio"), \
                   ((col("Total_wins") + col("Total_second_places") + col("Total_third_places")) / col("Total_rides")).alias("jockey_place_ratio"), \
                   ((col("Total_rides") - (col("Total_wins") + col("Total_second_places") + col("Total_third_places"))) / col("Total_rides")).alias("jockey_lose_ratio"))
    return df


year_threshold = 2015


# race_preprocessing
def race_preprocessing(df):
    def return_year(x):
        return int(str(x)[-4:])

    class_trans_dict = {
        'Hong Kong Group One': 'Group One',
        'Hong Kong Group Three': 'Group Three',
        'Group One': 'Group 1',
        'Class 4 (Special Condition)': 'Class 4',
        'Hong Kong Group Two': 'Group Two',
        'Class 4 (Restricted)': 'Class 4',
        'Class 3 (Special Condition)': 'Group 1',
        'Class 2 (Bonus Prize Money)': 'Class 2',
        'Class 3 (Bonus Prize Money)': 'Class 3',
        'Class 4 (Bonus Prize Money)': 'Class 4',
        '4 Year Olds ': '4 Year Olds',
        'Restricted Race': 'Griffin Race'}

    def map_race_class(x):
        if x in class_trans_dict.keys():
            return class_trans_dict[x]
        else:
            return x

    def fix_surface_type(x):
        if 'TURF' in str(x):
            return 'Turf'
        else:
            return 'All_Weather'

    returnyear_func = udf(return_year, IntegerType())
    map_race_class_func = udf(map_race_class, StringType())
    fix_surface_type_func = udf(fix_surface_type, StringType())

    df = df.withColumn("Year", returnyear_func(df["Date"]))
    df = df.select('*').where(f'Year > {year_threshold}')

    # getting distinct distance values
    distance_list = df.select("Distance").distinct().orderBy("Distance").rdd.map(lambda x: x.Distance).collect()

    # map class based on mapping dictionary
    df = df.withColumn("Class", map_race_class_func(df["Class"]))
    # Concatenate Class and Ranking
    df = df.select('*', concat_ws("_", "Class", "Ranking")) \
        .withColumnRenamed('concat_ws(_, Class, Ranking)', 'class_rank')
    # set Surface Type
    df = df.withColumn("Surface_Type", fix_surface_type_func(df["Surface"]))
    df = df.select('*', lit('Short').alias('Distance_Type'))
    df = df.withColumn("Distance_Type", when(((df["Course"] == 'Sha Tin') & (df["Surface_Type"] == "Turf") & (df["Distance"] > 1400) & (df['Distance'] <= 1800)), "Medium").otherwise(df["Distance_Type"]))
    df = df.withColumn("Distance_Type", when(((df["Course"] == 'Sha Tin') & (df["Surface_Type"] == "Turf") & (df['Distance'] > 1800)), "Long").otherwise(df["Distance_Type"]))
    df = df.withColumn("Distance_Type", when(((df["Course"] == 'Sha Tin') & (df["Surface_Type"] == "All_Weather") & (df['Distance'] > 1300)), "Medium").otherwise(df["Distance_Type"]))
    df = df.withColumn("Distance_Type", when(((df["Course"] == 'Happy Valley') & (df["Distance"] > 1200) & (df['Distance'] <= 1800)), "Medium").otherwise(df["Distance_Type"]))
    df = df.withColumn("Distance_Type", when(((df["Course"] == 'Happy Valley')  & (df["Distance"] > 1800)), "Long").otherwise(df["Distance_Type"]))
    return df


# record preprocessing

def record_preprocessing(df, df_races, df_horses):
    def parse_placings(x):
        return int(x.split(" ")[0])

    def parse_finish_time(x):
        #         print(str(x))
        time = int(x[:2]) * 60 * 100 + int(x[3:5]) * 100 + int(x[6:])
        time = time / 1000
        return time

    splitfunc = udf(parse_placings, IntegerType())
    convert_time = udf(parse_finish_time, FloatType())

    print('Start')
    df = df.select('*').where(
        "Place != 'DISQ' AND Place != 'DNF' AND Place != 'FE' AND Place != 'PU' AND Place != 'TNP' AND Place != 'UR' AND Place != 'VOID' AND Place != 'WR' AND Place != 'WV' AND Place != 'WV-A' AND Place != 'WX' AND Place != 'WX-A' AND Place != 'WXNR' AND Place IS NOT NULL")
    df = df.withColumn("Place", splitfunc(df["Place"]))
    df = df.select('*').where('Place != 99 AND Place != 47')
    print('Dropped 99,47 and other Place values')

    df = df.select('*').where('Finish_time is not null').where('Place_Section_1 is not null')
    df = df.withColumn("Finish_time_mille_second", convert_time(df['Finish_time']))
    print('Parsed the finishing time of the dataset')

    df = df.drop('Record_ID', 'Horse_Number', 'Horse_Code')
    df = df.withColumn('Win_odds', col('Win_odds').cast(FloatType()))
    df = df.join(df_races.select('Race_ID', 'Course', 'Prize', 'Date', "Distance_Type", "Class", "Ranking", "Surface_Type"), 'Race_ID')
    df = df.join(df_horses.select("Horse_ID", "Age", "State", "Sex", "Foal_Date"), "Horse_ID")
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    df = df.withColumn('Date', to_date(col('Date'), "M/dd/yyyy"))
    print('Cast Date')

    df = df.drop('Age')
    first_race_date_df = df.select('Horse_ID', 'Date'). \
        groupby('Horse_ID').agg(min('Date')). \
        withColumnRenamed('min(Date)', 'First_Race_Date'). \
        orderBy('Horse_ID')
    df = df.join(first_race_date_df, "Horse_ID")

    # can be changed with average of foal date
    # use a table with foal date and age_at_first_race by using the first_race_date_df
    # floor(datediff(col("First_race_date_df"),col("Foal Date"))/365)
    # join the above with df
    # fill the missing values with the mean value for age_at_first_race
    df = df.select('*', (3 + floor(datediff(col("Date"), col("First_Race_Date")) / 365)). \
                   alias('Age_At_Race'))

    # making Win Odds into a value between 0 and 1
    df_sum_win_odds_reciprocal = df.select("Race_ID", "Horse_ID", "Win_odds", "Prize", (1 / col("Win_odds")).alias('Reciprocal Win Odds')) \
        .groupBy("Race_ID").sum("Reciprocal Win Odds") \
        .withColumnRenamed('sum(Reciprocal Win Odds)', 'Sum Reciprocal') \
        .orderBy("Race_ID")

    # As we have the sum of reciprocal of the Win Odds of each race, we can divide
    # the price money by this sum, to get the money available after HKJC takes
    # it's commission
    print('Age at race done')
    df = df.withColumn("Prize", regexp_replace("Prize", ",", "").cast(IntegerType())) \
        .select('Race_ID', "Horse_ID", "Weight", "Weight_Declared", \
                "Win_odds", "Draw", "Place", "Prize", "Course", "Surface_Type", "Distance_Type", "Class", "Ranking", "Date", "State", "Sex", "First_Race_Date", \
                "Age_At_Race", "Jockey_ID", "Trainer_ID", "Finish_time_mille_second") \
        .join(df_sum_win_odds_reciprocal, "Race_ID") \
        .select('Race_ID', "Horse_ID", "Weight", "Weight_Declared", \
                "Win_odds", "Draw", "Place", "Prize", "Course", "Surface_Type", "Distance_Type", "Class", "Ranking", "Date", "State", "Sex", "First_Race_Date", \
                "Age_At_Race", "Jockey_ID", "Trainer_ID", (col("Prize") / col("Sum Reciprocal")).alias("Available Prize Money"), "Finish_time_mille_second") \
        .select('Race_ID', "Horse_ID", "Weight", "Weight_Declared", \
                "Win_odds", "Draw", "Place", "Prize", "Course", "Surface_Type", "Distance_Type", "Class", "Ranking", "Date", "State", "Sex", "First_Race_Date", \
                "Age_At_Race", "Jockey_ID", "Trainer_ID", ((col("Available Prize Money") / col("Win_odds")) / col("Prize")).alias("Win_odds_%"), "Finish_time_mille_second") \
        .orderBy("Race_ID")
    print('Win odds calculated')
    # drop weight_declared as it has too many missing values
    df = df.drop('Weight_Declared')
    # As Weight declared has only 12 pieces of data with '---', we drop these too
    df = df.select("*").where("Weight != '---'")

    # There are races with only 1 to 4 competitors. These will be dropped
    df_low_placings = None
    for i in range(1, 5):
        if df_low_placings == None:
            df_low_placings = df.select("Race_ID", "Place") \
                .groupby("Race_ID") \
                .agg(max("Place")) \
                .withColumnRenamed("max(Place)", "Place") \
                .select("Race_ID", "Place") \
                .where(f"Place = {i}")
            df_low_placings.cache()
        else:
            df_low_placings = df_low_placings.union(df.select("Race_ID", "Place") \
                                                    .groupby("Race_ID") \
                                                    .agg(max("Place")) \
                                                    .withColumnRenamed("max(Place)", "Place") \
                                                    .select("Race_ID", "Place") \
                                                    .where(f"Place = {i}"))
            df_low_placings.cache()
    # list of race ids with only 1 to 4 competitors
    race_id_list = df_low_placings.select("Race_ID").rdd.map(lambda x: x.Race_ID).collect()
    df = df.select('Race_ID', "Horse_ID", "Weight", "Age_At_Race", \
                   "Win_odds", "Win_odds_%", "Draw", "Place", "Prize", "Surface_Type", "Distance_Type", "Class", "Ranking", "Course", "Date", "State", "Sex", "First_Race_Date", "Jockey_ID", "Trainer_ID", "Finish_time_mille_second") \
        .where(~col("Race_ID").isin(race_id_list))
    return df


# horse_preprocessing
def horse_preprocessing(df):
    df = df.select('Horse_ID', 'Horse_Name', 'State', 'Country', 'Age', 'Color', 'Sex', 'Import_type', 'Total_Stakes', 'Last_Rating')
    return df


# sectional_preprocessing
def sectional_preprocessing(df):
    df = df.select('Race_ID', 'Horse_ID', 'Finish_time')
    return df


def foal_preprocessing(df_horse, df_foal):
    df = df_horse.join(df_foal.select('Horse_ID', 'Foal_Date'), 'Horse_ID', 'left')
    return df


def calculate_win_percentage(partition):
    for horse in partition:
        horse_id = horse[0]
        win_count = 0
        total_count = 0
        win_percentage = list()
        for i in horse[1]:
            total_count += 1
            # i is a tuple having race_id,data,place
            if i[2] == 1:
                win_count += 1
            win_percentage.append((horse_id, i[0], i[1], (win_count / total_count) * 100))

        yield (horse_id, win_percentage)


def calculate_place_percentage(partition):
    for horse in partition:
        horse_id = horse[0]
        place_count = 0
        total_count = 0
        place_percentage = list()
        for i in horse[1]:
            total_count += 1
            # i is a tuple having race_id,data,place
            if (i[2] == 1) or (i[2] == 2) or (i[2] == 3):
                place_count += 1
            place_percentage.append((horse_id, i[0], i[1], (place_count / total_count) * 100))

        yield (horse_id, place_percentage)


# Example of Divide and Conquer being used
# pass in the records df to this function
def get_win_and_place_percentage_df(df, spark):
    sc = spark.sparkContext
    df_horse_place = df.select('Race_ID', 'Horse_ID', 'Date', 'Place'). \
        groupby('Horse_ID', 'Race_ID', 'Date').agg(max(col('Place'))). \
        withColumnRenamed('max(Place)', 'Place'). \
        orderBy('Horse_ID', 'Date')
    # difficult to apply pandas type operations on sparksql
    # requires pyarrow which isnt installing
    # Turn to RDD and use Divide and conquer
    horse_place_rdd = df_horse_place.rdd
    horse_place_rdd = horse_place_rdd.map(lambda x: (x.Horse_ID, (x.Race_ID, x.Date, x.Place)))

    # Group by key to get all races that a horse has participated in
    # Key is horse ID
    # Make the values to a list format while maintaining the partitioning
    # that we get by groupByKey by using mapValues
    grouped_horse_id_rdd = horse_place_rdd.groupByKey().mapValues(list)

    # apply the mapPartitions method to do D&C
    win_percent = grouped_horse_id_rdd.mapPartitions(calculate_win_percentage)
    place_percent = grouped_horse_id_rdd.mapPartitions(calculate_place_percentage)

    # result is mapped to get only the values from the key,value pair
    # then we flatMap it to get to rdd format for dataframe
    win_percent_rdd = win_percent.map(lambda x: x[1]).flatMap(lambda x: x)
    place_percent_rdd = place_percent.map(lambda x: x[1]).flatMap(lambda x: x)
    schema_win_percent = StructType([
        StructField("Horse_ID", IntegerType(), True),
        StructField("Race_ID", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("Win_Perc", FloatType(), True)
    ])

    win_percent_dataframe = spark.createDataFrame(win_percent_rdd, schema_win_percent)

    schema_place_percent = StructType([
        StructField("Horse_ID", IntegerType(), True),
        StructField("Race_ID", IntegerType(), True),
        StructField("Date", DateType(), True),
        StructField("Place_Perc", FloatType(), True)
    ])

    place_percent_dataframe = spark.createDataFrame(place_percent_rdd, schema_place_percent)

    return win_percent_dataframe, place_percent_dataframe


def get_weather_data(spark):
    pipeline1 = "{'$project': {'day': 1,'month':1,'year':1,'sha_tin_max':1,'sha_tin_min':1,'_id':0}}"
    pipeline2 = "{'$project': {'day': 1,'month':1,'year':1,'happy_velley_max':1,'happy_velley_min':1,'_id':0}}"
    df1 = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", "HorseRacing") \
        .option("collection", "WeatherData").option('pipeline', pipeline1).load()

    df2 = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", "HorseRacing") \
        .option("collection", "WeatherData").option('pipeline', pipeline2).load()
    # raw = spark.read.csv('weather_data.csv',header=True, inferSchema=True)

    # df1 = raw.select('day','month','year','sha_tin_max','sha_tin_min','_c0')
    # df2 = raw.select('day','month','year',"happy_velley_max",'happy_velley_min','_c0')

    df1 = df1.select('*', lit('Sha Tin').alias('Weather_Course')) \
        .withColumnRenamed('sha_tin_max', 'MaxTemp') \
        .withColumnRenamed('sha_tin_min', 'MinTemp') \
        .withColumnRenamed('_c0', '_id')

    df2 = df2.select('*', lit('Happy Valley').alias('Weather_Course')) \
        .withColumnRenamed('happy_velley_max', 'MaxTemp') \
        .withColumnRenamed('happy_velley_min', 'MinTemp') \
        .withColumnRenamed('_c0', '_id')

    df1 = df1.select(concat_ws('/', df1.month.cast(IntegerType()), df1.day.cast(IntegerType()), df1.year.cast(IntegerType())).alias('Weather_Date'), 'Weather_Course', 'MaxTemp', 'MinTemp')
    df2 = df2.select(concat_ws('/', df2.month.cast(IntegerType()), df2.day.cast(IntegerType()), df2.year.cast(IntegerType())).alias('Weather_Date'), 'Weather_Course', 'MaxTemp', 'MinTemp')
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    df1 = df1.withColumn('Weather_Date', to_date(col('Weather_Date'), "M/dd/yyyy"))
    df2 = df2.withColumn('Weather_Date', to_date(col('Weather_Date'), "M/dd/yyyy"))
    return df1.union(df2)


def write_dataset_to_mongoDB(spark, df, dbname, collectionname):
    try:
        df.write.format("com.mongodb.spark.sql.DefaultSource") \
            .mode("append") \
            .option("database", dbname) \
            .option("collection", collectionname) \
            .save()
        return True
    except BaseException as err:
        return f"Unexpected {err=}, {type(err)=}"


def read_from_mongoDB(spark, dbname, collectionname):
    try:
        df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
            .option("database", dbname) \
            .option("collection", collectionname).load()
        return df
    except BaseException as err:
        return f"Unexpected {err=}, {type(err)=}"


def model_and_predict(df_dataset):
    temp = df_dataset.select('Date').select(max('Date')).withColumnRenamed('max(Date)', 'max_date')
    df_dataset = df_dataset.join(temp)
    trainingData = df_dataset.select('*').where('Date != max_date')
    testData = df_dataset.select('*').where('Date == max_date')

    VectorAssm = VectorAssembler(inputCols=features_cols, outputCol='features')
    df_vector_dataset = VectorAssm.transform(df_dataset)

    scaler = MinMaxScaler(inputCol="features", outputCol="features_scaled")
    df_vector_dataset_scaled = scaler.fit(df_vector_dataset).transform(df_vector_dataset)

    featureIndexer = VectorIndexer(inputCol="features_scaled", outputCol="indexedFeatures").fit(df_vector_dataset_scaled)

    gbt = GBTRegressor(featuresCol="indexedFeatures", labelCol="Target", maxIter=20)
    # rf = RandomForestRegressor(featuresCol="indexedFeatures", labelCol="Target")

    pipeline = Pipeline(stages=[VectorAssm, scaler, featureIndexer, gbt])
    # pipeline_rf = Pipeline(stages=[featureIndexer, rf])

    model = pipeline.fit(trainingData)
    # model_rf = pipeline_rf.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    # Select (prediction, true label) and compute test error
    evaluator = RegressionEvaluator(labelCol="Target", predictionCol="prediction", metricName="rmse")

    rmse = evaluator.evaluate(predictions)
    print("GBT Root Mean Squared Error (RMSE) on test data = %g" % (rmse * 10))

    return predictions, rmse


if __name__ == "__main__":
    # initialize spark context
    spark = SparkSession.builder.master('spark://vm1:7077').appName("Data Processing_Horse Racing").config("spark.mongodb.input.uri", "mongodb://20.187.94.145/HorseRacing.WeatherData") \
        .config("spark.mongodb.output.uri", "mongodb://20.187.94.145/HorseRacing.WeatherData") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1').getOrCreate()
    df_races, df_races_sectional, df_trainer, df_jockeys, df_records, df_horse, df_sectional, df_foal = read_data(spark)

    df_trainer = trainer_preprocessing(df_trainer)
    df_jockeys = jockey_preprocessing(df_jockeys)
    df_races = race_preprocessing(df_races)
    df_horse = horse_preprocessing(df_horse)
    df_horse = foal_preprocessing(df_horse, df_foal)

    # note that in records_preprocessing, races and horse are already joined into the dataframe
    df_records = record_preprocessing(df_records, df_races, df_horse)

    # Connects to mongodb and gets the Weather data
    df_weather = get_weather_data(spark)
    df_records = df_records.join(df_weather, (df_records["Date"] == df_weather["Weather_Date"]) & (df_records["Course"] == df_weather["Weather_Course"]))

    df_records_jockey = df_records.join(df_jockeys, "Jockey_ID", 'left')
    df_records_jockey_trainer = df_records_jockey.join(df_trainer, "Trainer_ID", 'left')
    df_win_percent, df_place_percent = get_win_and_place_percentage_df(df_records, spark)
    df_records_jockey_trainer_win = df_records_jockey_trainer.join(df_win_percent, ['Horse_ID', 'Race_ID', 'Date'])
    df_records_jockey_trainer_win_place = df_records_jockey_trainer_win.join(df_place_percent, ['Horse_ID', 'Race_ID', 'Date'])
    print(df_records_jockey_trainer_win_place.count())

    df_dataset = df_records_jockey_trainer_win_place.select('Date', 'Race_ID', 'Horse_ID', 'Trainer_ID', 'Jockey_ID', 'Weight', 'Age_At_Race', 'Win_odds', 'Win_odds_%', 'Draw', \
                                                            'Prize', 'Surface_Type', 'Distance_Type', "Class", "Ranking", \
                                                            'Course', 'Sex', 'MaxTemp', 'MinTemp', 'jockey_first_place_ratio', \
                                                            'jockey_second_place_ratio', 'jockey_third_place_ratio', \
                                                            'jockey_place_ratio', 'trainer_first_place_ratio', 'trainer_second_place_ratio',
                                                            'trainer_third_place_ratio', 'trainer_place_ratio', 'Win_Perc', 'Place_Perc', \
                                                            'Finish_time_mille_second')
    df_dataset = df_dataset.withColumn("Weight", df_dataset.Weight.cast(FloatType()))
    df_dataset = df_dataset.withColumn("Draw", df_dataset.Weight.cast(FloatType()))
    df_dataset = df_dataset.withColumnRenamed("Finish_time_mille_second", "Target")

    df_dataset = df_dataset.select('*').where('Ranking is not null and Sex is not null and Course is not null and Class is not null and Distance_Type is not null and Surface_Type is not null')

    features_cols = ["Weight", "Age_At_Race", "Win_odds", "Win_odds_%", "Draw", "Prize", "Surface_Type_index", "Distance_Type_index",
                     "Class_index", "Ranking_index", "Course_index", "Sex_index", "MaxTemp", "MinTemp", "jockey_first_place_ratio", "jockey_second_place_ratio",
                     "jockey_third_place_ratio", "jockey_place_ratio", "trainer_first_place_ratio", "trainer_second_place_ratio",
                     "trainer_third_place_ratio", "trainer_place_ratio", "Win_Perc", "Place_Perc"]

    string_cols = ["Surface_Type", "Distance_Type", "Class", "Ranking", "Course", "Sex"]
    string_cols_idx = ["Surface_Type_index", "Distance_Type_index", "Class_index", "Ranking_index", "Course_index", "Sex_index"]

    StringIdxer = StringIndexer(inputCols=string_cols, outputCols=string_cols_idx)

    df_dataset_to_mongo = StringIdxer.fit(df_dataset).transform(df_dataset)

    # mongo insertion
    # we use the mongodb instance in our vm1 as a form of intermediate storage
    # such that when we need to retrain/predict results for previous races,
    # we can use the same stored data without having to reread from HDFS and reprocessing
    insertionstatus = write_dataset_to_mongoDB(spark, df_dataset_to_mongo, "HorseRacing", "HorseRacingProcessedDataSet")

    if insertionstatus == True:
        # read back from MongoDB
        df_dataset2 = read_from_mongoDB(spark, "HorseRacing", "HorseRacingProcessedDataSet")

        if ~isinstance(df_dataset2, str):

            predictions, rmse = model_and_predict(df_dataset2)

            # produce rmse of prediction
            with open('rmse.txt', 'w') as lala:
                lala.write(str(rmse))

            output = predictions.select('Date', 'Race_ID', 'Horse_ID', 'Trainer_ID', 'Jockey_ID', 'Weight', 'Win_odds', 'Draw', col('prediction') * lit(10)).withColumnRenamed('(prediction * 10)', 'predicted finishing time')
            df_races_out, df_races_sectional_out, df_trainer_out, df_jockeys_out, df_records_out, df_horse_out, df_sectional_out, df_foal_out = read_data(spark)

            df_jockeys_out = df_jockeys_out.withColumnRenamed("name", "Jockey_name")
            df_trainer_out = df_trainer_out.withColumnRenamed("name", "Trainer_name")

            output = output.join(df_races_out.select('Race_ID', 'Race_Type'), 'Race_ID', 'left')
            output = output.join(df_horse_out.select('Horse_ID', 'Horse_Name'), 'Horse_ID', 'left')
            output = output.join(df_trainer_out.select('Trainer_ID', 'Trainer_name'), 'Trainer_ID', 'left')
            output = output.join(df_jockeys_out.select('Jockey_ID', 'Jockey_name'), 'Jockey_ID', 'left')

            output_temp = output.withColumn('Place', f.row_number().over(
                Window.partitionBy("Race_ID").orderBy(col("predicted finishing time").asc())))

            output_final = output_temp.select('Race_Type', 'Horse_Name', 'Trainer_name', 'Jockey_name', 'Weight', 'Draw', 'Win_odds', 'predicted finishing time', 'Place')

            output_final.show(output_final.count())

            today_date = date.today().strftime("%Y-%m-%d")
            pred_date = output.select('Date').distinct().rdd.map(lambda row: [str(c) for c in row]).collect()[0][0]
            output_table = output_final.rdd.map(lambda row: [str(c) for c in row]).collect()
            spark.stop()

            app = Flask(__name__)


            @app.route("/")
            def prediction_results():
                return render_template('main.html', Date=date.today().strftime("%Y-%m-%d"), Pred_date=pred_date, result=output_table)


            app.run(host='127.0.0.1', port=5000, debug=True)


        else:
            print(df_dataset2)
            spark.stop()
    else:
        print(insertionstatus)
        spark.stop()

    # take from mongo the required train set
