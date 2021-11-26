import numpy as np
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType


#import data
def read_data(spark):
    directory_path1 = os.path.join(os.getcwd(),"Full_Data_Pack_1")
    directory_path2 = os.path.join(os.getcwd(),"Full_Data_Pack_2")
    
    df_horse = spark.read.csv(os.path.join(directory_path1,"horses","horses.csv"), header=True, inferSchema=True)
    df_jockey = spark.read.csv(os.path.join(directory_path1,"jockeys","jockeys.csv"), header=True, inferSchema=True)
    df_races_sectional = spark.read.csv(os.path.join(directory_path1,"races_sectional","races_sectional.csv"), header=True, inferSchema=True)
    df_trainer = spark.read.csv(os.path.join(directory_path1,"trainer","trainer.csv"), header=True, inferSchema=True)
    df_sectional = spark.read.csv(os.path.join(directory_path1,"sectional_table","sectional_table.csv"), header=True, inferSchema=True)
    df_records = spark.read.csv(os.path.join(directory_path2,"records","records.csv"), header=True, inferSchema=True)
    df_races = spark.read.csv(os.path.join(directory_path2,"races","races.csv"), header=True, inferSchema=True)
    
    return df_races, df_races_sectional, df_trainer, df_jockey, df_records, df_horse, df_sectional

#trainer preprocessing
def trainer_preprocessing(df):
    df = df.withColumn('Total_wins',when(df_trainer["Total_wins"].isNull(),0).\
                      otherwise(df_trainer["Total_wins"]))
    df = df.withColumn('Total_second_places',when(df_trainer["Total_second_places"].isNull(),0).\
                      otherwise(df_trainer["Total_second_places"]))
    df = df.withColumn('Total_third_places',when(df_trainer["Total_third_places"].isNull(),0).\
                      otherwise(df_trainer["Total_third_places"]))
    df = df.select("Trainer_ID","Total_wins","Total_second_places","Total_third_places","Total_rides")
    df = df.select("Trainer_ID",(col("Total_wins")/col("Total_rides")).alias("trainer_first_place_ratio"),\
                                (col("Total_second_places")/col("Total_rides")).alias("trainer_second_place_ratio"),\
                                (col("Total_third_places")/col("Total_rides")).alias("trainer_third_place_ratio"),\
                                ((col("Total_wins") + col("Total_second_places") + col("Total_third_places"))/col("Total_rides")).alias("trainer_place_ratio"),\
                                ((col("Total_rides") - (col("Total_wins") + col("Total_second_places") + col("Total_third_places")))/col("Total_rides")).alias("trainer_lose_ratio"))
    return df

#jockey preprocessing
def jockey_preprocessing(df):
    df = df.withColumn('Total_wins',when(df_trainer["Total_wins"].isNull(),0).\
                      otherwise(df_trainer["Total_wins"]))
    df = df.withColumn('Total_second_places',when(df_trainer["Total_second_places"].isNull(),0).\
                      otherwise(df_trainer["Total_second_places"]))
    df = df.withColumn('Total_third_places',when(df_trainer["Total_third_places"].isNull(),0).\
                      otherwise(df_trainer["Total_third_places"]))
    df = df.select("Jockey_ID","Total_wins","Total_second_places","Total_third_places","Total_rides")
    df = df.select("Jockey_ID",(col("Total_wins")/col("Total_rides")).alias("trainer_first_place_ratio"),\
                                (col("Total_second_places")/col("Total_rides")).alias("trainer_second_place_ratio"),\
                                (col("Total_third_places")/col("Total_rides")).alias("trainer_third_place_ratio"),\
                                ((col("Total_wins") + col("Total_second_places") + col("Total_third_places"))/col("Total_rides")).alias("trainer_place_ratio"),\
                                ((col("Total_rides") - (col("Total_wins") + col("Total_second_places") + col("Total_third_places")))/col("Total_rides")).alias("trainer_lose_ratio"))
    return df


#race_preprocessing
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
        
    returnyear_func = udf(return_year,IntegerType())
    map_race_class_func = udf(map_race_class,StringType())
    fix_surface_type_func = udf(fix_surface_type,StringType())
    
    df = df.withColumn("Year",returnyear_func(df["Date"]))
    df = df.select('*').where('Year > 2008')
    #getting distinct distance values
    distance_list = df.select("Distance").distinct().orderBy("Distance").rdd.map(lambda x:x.Distance).collect()
    
    #map class based on mapping dictionary
    df = df.withColumn("Class",map_race_class_func(df["Class"]))
    #Concatenate Class and Ranking
    df = df.select('*',concat_ws("_","Class","Ranking"))\
           .withColumnRenamed('concat_ws(_, Class, Ranking)','class_rank')
    #set Surface Type
    df = df.withColumn("Surface_Type",fix_surface_type_func(df["Surface"]))
    df = df.select('*',lit('Short').alias('Distance_Type'))
    df = df.withColumn("Distance_Type",when(((df["Course"] == 'Sha Tin') & (df["Surface_Type"] == "Turf")\
                  & (df["Distance"]  > 1400) & (df['Distance'] <= 1800)),"Medium").\
                       otherwise(df["Distance_Type"]))
    df = df.withColumn("Distance_Type",when(((df["Course"] == 'Sha Tin') & (df["Surface_Type"] == "Turf")\
              & (df['Distance'] > 1800)),"Long").\
                   otherwise(df["Distance_Type"]))
    df = df.withColumn("Distance_Type",when(((df["Course"] == 'Sha Tin') & (df["Surface_Type"] == "All_Weather")\
              & (df['Distance'] > 1300)),"Medium").\
                   otherwise(df["Distance_Type"]))    
    df = df.withColumn("Distance_Type",when(((df["Course"] == 'Happy Valley')\
                  & (df["Distance"]  > 1200) & (df['Distance'] <= 1800)),"Medium").\
                       otherwise(df["Distance_Type"]))
    df = df.withColumn("Distance_Type",when(((df["Course"] == 'Happy Valley')\
                  & (df["Distance"]  > 1800)),"Long").\
                       otherwise(df["Distance_Type"]))
    return df

#record preprocessing

def record_preprocessing(df,df_races,df_horses):
    def parse_placings(x): 
        return int(x.split(" ")[0])
    
    splitfunc = udf(parse_placings,IntegerType())
    print('Start')
    df = df.select('*').where("Place != 'DISQ' AND Place != 'DNF' AND Place != 'FE' AND Place != 'PU' AND Place != 'TNP' AND Place != 'UR' AND Place != 'VOID' AND Place != 'WR' AND Place != 'WV' AND Place != 'WV-A' AND Place != 'WX' AND Place != 'WX-A' AND Place != 'WXNR' AND Place IS NOT NULL")
    df = df.withColumn("Place",splitfunc(df["Place"]))
    df = df.select('*').where('Place != 99 AND Place != 47')
    print('Dropped 99,47 and other Place values')
    
    df = df.drop('Record_ID','Horse_Number','Horse_Code')
    df = df.withColumn('Win_odds',col('Win_odds').cast(FloatType()))
    df = df.join(df_races.select('Race_ID','Prize','Date'),'Race_ID')
    df = df.join(df_horses.select("Horse_ID","Age","State","Sex"),"Horse_ID")
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    df = df.withColumn('Date',to_date(col('Date'),"M/dd/yyyy"))
    print('Cast Date')
    df = df.drop('Age')
    first_race_date_df = df.select('Horse_ID','Date').\
                    groupby('Horse_ID').agg(min('Date')).\
                    withColumnRenamed('min(Date)','First_Race_Date').\
                    orderBy('Horse_ID')
    df = df.join(first_race_date_df,"Horse_ID")
    #can be changed with average of foal date
    #use a table with foal date and age_at_first_race by using the first_race_date_df
    #floor(datediff(col("First_race_date_df"),col("Foal Date"))/365)
    #join the above with df
    #fill the missing values with the mean value for age_at_first_race
    df = df.select('*',(3 + floor(datediff(col("Date"),col("First_Race_Date"))/365)).\
                        alias('Age_At_Race'))
    #making Win Odds into a value between 0 and 1
    df_sum_win_odds_reciprocal = df.select("Race_ID","Horse_ID","Win_odds","Prize",(1/col("Win_odds")).alias('Reciprocal Win Odds'))\
                     .groupBy("Race_ID").sum("Reciprocal Win Odds")\
                     .withColumnRenamed('sum(Reciprocal Win Odds)','Sum Reciprocal')\
                     .orderBy("Race_ID")
    #As we have the sum of reciprocal of the Win Odds of each race, we can divide
    #the price money by this sum, to get the money available after HKJC takes 
    #it's commission
    print('Age at race done')
    df = df.withColumn("Prize",regexp_replace("Prize",",","").cast(IntegerType()))\
                     .select('Race_ID',"Horse_ID","Weight","Weight_Declared",\
                            "Win_odds","Draw","Place","Prize","Date","State","Sex","First_Race_Date",\
                            "Age_At_Race")\
                     .join(df_sum_win_odds_reciprocal,"Race_ID")\
                     .select('Race_ID',"Horse_ID","Weight","Weight_Declared",\
                            "Win_odds","Draw","Place","Prize","Date","State","Sex","First_Race_Date",\
                            "Age_At_Race",(col("Prize")/col("Sum Reciprocal")).alias("Available Prize Money"))\
                     .select('Race_ID',"Horse_ID","Weight","Weight_Declared",\
                            "Win_odds","Draw","Place","Prize","Date","State","Sex","First_Race_Date",\
                            "Age_At_Race",((col("Available Prize Money")/col("Win_odds"))/col("Prize")).alias("Win_odds_%"))\
                     .orderBy("Race_ID")
    print('Win odds calculated')
    #drop weight_declared as it has too many missing values
    df = df.drop('Weight_Declared')
    #As Weight declared has only 12 pieces of data with '---', we drop these too
    df = df.select("*").where("Weight != '---'")
    
    #There are races with only 1 to 4 competitors. These will be dropped
    df_low_placings = None
    for i in range(1,5):
        if df_low_placings == None:
            df_low_placings = df.select("Race_ID","Place")\
                             .groupby("Race_ID")\
                             .agg(max("Place"))\
                             .withColumnRenamed("max(Place)","Place")\
                             .select("Race_ID","Place")\
                             .where(f"Place = {i}")
            df_low_placings.cache()
        else:
            df_low_placings = df_low_placings.union(df.select("Race_ID","Place")\
                             .groupby("Race_ID")\
                             .agg(max("Place"))\
                             .withColumnRenamed("max(Place)","Place")\
                             .select("Race_ID","Place")\
                             .where(f"Place = {i}"))
            df_low_placings.cache()
    #list of race ids with only 1 to 4 competitors
    race_id_list = df_low_placings.select("Race_ID").rdd.map(lambda x:x.Race_ID).collect()
    df = df.select('Race_ID',"Horse_ID","Weight","Age_At_Race",\
                            "Win_odds","Win_odds_%","Draw","Place","Prize","Date","State","Sex","First_Race_Date",\
                            ).where(~col("Race_ID").isin(race_id_list))
    return df


#horse_preprocessing
def horse_preprocessing(df):
    df = df.select('Horse_ID', 'State', 'Country', 'Age', 'Color', 'Sex', 'Import_type', 'Total_Stakes', 'Last_Rating')
    return df

#sectional_preprocessing
def sectional_preprocessing(df):
    df = df.select('Race_ID', 'Horse_ID', 'Finish_time')
    return df


if __name__ == "__main__":
	#initialize spark context
	spark = SparkSession.builder.appName("Data Processing").getOrCreate()

	df_races, df_races_sectional, df_trainer, df_jockeys, df_records, df_horse, df_sectional = read_data(spark)
    	df_trainer = trainer_preprocessing(df_trainer)
    	df_jockeys = jockey_preprocessing(df_jockeys)
    	df_races = race_preprocessing(df_races)
        #note that in records_preprocessing, races and horse are already joined into the dataframe
    	df_records = record_preprocessing(df_records)
    	df_horse = horse_preprocessing(df_horse)
    	df_sectional = sectional_preprocessing(df_sectional)

	print(df_records.count())
	df_records_jockey = df_records.join(df_jockeys,"Jockey_ID",'left')
	print(df_records_jockey.count())
	df_records_jockey_trainer = df_records_jockey.join(df_trainer,"Trainer_ID",'left')
	print(df_records_jockey_trainer.count())
	#df_records_jockey_trainer_race = df_records_jockey_trainer.join(df_races,'Race_ID','left')
	#print(df_records_jockey_trainer_race.count())
	#df_records_jockey_trainer_race_horse = df_records_jockey_trainer_race.join(df_horse,'Horse_ID','left')
	#print(df_records_jockey_trainer_race_horse.count())
	df_records_jockey_trainer_race_horse_sectional = df_records_jockey_trainer.join(df_sectional,["Horse_ID","Race_ID"],'left')
	print(df_records_jockey_trainer_race_horse_sectional.count())

	spark.stop()
	



