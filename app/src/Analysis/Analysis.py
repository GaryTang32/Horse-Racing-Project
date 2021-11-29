import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
# from dao import weather_data_dao

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def read_data():
    prefix = r'../Dataset/'
    path_horse = prefix + 'horses.csv'
    path_jockeys = prefix + 'jockeys.csv'
    path_races = prefix + 'races.csv'
    path_races_sectional = prefix + 'races_sectional.csv'
    path_trainer = prefix + 'trainer.csv'
    path_records = prefix + 'records.csv'
    path_section_table = prefix + 'sectional_table.csv'

    df_races = pd.read_csv(path_races)
    df_races_sectional = pd.read_csv(path_races_sectional)
    df_trainer = pd.read_csv(path_trainer)
    df_jockey = pd.read_csv(path_jockeys)
    df_records = pd.read_csv(path_records)
    df_horse = pd.read_csv(path_horse)
    df_sectional = pd.read_csv(path_section_table)
    return df_races, df_races_sectional, df_trainer, df_jockey, df_records, df_horse, df_sectional


def trainer_preprocessing(df):
    # for trainer, only calculate its winning ratio

    df = df.fillna(0)
    df['trainer_first_place_ratio'] = df['Total_wins'] / df['Total_rides']
    df['trainer_second_place_ratio'] = df['Total_second_places'] / df['Total_rides']
    df['trainer_third_place_ratio'] = df['Total_third_places'] / df['Total_rides']
    df['trainer_place_ratio'] = (df['Total_wins'] + df['Total_second_places'] + df['Total_third_places']) / df['Total_rides']
    df['trainer_lose_ratio'] = 1 - df['trainer_place_ratio']
    cols = ['Trainer_ID', 'trainer_first_place_ratio', 'trainer_second_place_ratio', 'trainer_third_place_ratio', 'trainer_place_ratio', 'trainer_lose_ratio']
    df = df[cols]
    return df


def jockeys_preprocessing(df):
    df['jockeys_first_place_ratio'] = df['Total_wins'] / df['Total_rides']
    df['jockeys_second_place_ratio'] = df['Total_second_places'] / df['Total_rides']
    df['jockeys_third_place_ratio'] = df['Total_third_places'] / df['Total_rides']
    df['jockeys_place_ratio'] = (df['Total_wins'] + df['Total_second_places'] + df['Total_third_places']) / df['Total_rides']
    df['jockeys_lose_ratio'] = 1 - df['jockeys_place_ratio']
    cols = ['Jockey_ID', 'jockeys_first_place_ratio', 'jockeys_second_place_ratio', 'jockeys_third_place_ratio', 'jockeys_place_ratio', 'jockeys_lose_ratio']
    df = df[cols].copy()
    return df


def race_preprocessing(df):
    df['Year'] = df['Date'].apply(lambda x: str(x)[:4])
    print(df.shape[0])
    df = df[df['Year'] > '2008'].copy()
    print(df.shape[0])
    print(set(df['Distance'].values.tolist()))
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
        'Restricted Race': 'Griffin Race'
    }
    for i in class_trans_dict.keys():
        df.loc[df['Class'] == i, 'Class'] = class_trans_dict[i]
    df['class_rank'] = df['Class'] + df['Ranking']


    df['Surface_Type'] = df['Surface'].apply(lambda x : 'Turf' if 'Turf' in str(x) else 'All_Weather')
    df['Distance_Type'] = 'Short'

    print(set(df['Distance'].values.tolist()))

    df['Distance'] = df['Distance'].astype('int64')


    df.loc[ (df['Course'] == 'Sha Tin') &
            (df['Surface_Type'] == 'Turf') &
            (df['Distance'] > 1400 ) &
            (df['Distance'] <= 1800), 'Distance_Type'] = 'Medium'

    df.loc[ (df['Course'] == 'Sha Tin') &
            (df['Surface_Type'] == 'Turf') &
            (df['Distance'] > 1800) , 'Distance_Type'] = 'Long'

    df.loc[ (df['Course'] == 'Sha Tin') &
            (df['Surface_Type'] == 'All_Weather') &
            (df['Distance'] > 1300) , 'Distance_Type'] = 'Medium'

    df.loc[ (df['Course'] == 'Happy Valley') &
            (df['Distance'] > 1200 ) &
            (df['Distance'] <= 1800), 'Distance_Type'] = 'Medium'

    df.loc[ (df['Course'] == 'Happy Valley') &
            (df['Distance'] > 1800), 'Distance_Type'] = 'Long'


    cols = ['Race_ID', 'Date', 'Class', 'Distance', 'Ranking', 'Going', 'Surface', 'Runners', 'class_rank',
            'Distance_Type', 'Surface_Type']
    df = df[cols]

    return df

def record_preprocessing(df):

    # Process Place
    df['Place'] = df['Place'].apply(lambda x : str(x).split()[0] )
    df = df[ df['Place'].apply(lambda x: True if ((str(x).isnumeric()) and (int(x) < 17)) else False ) == True ]



    #TODO: Many weight declaired is null
    # Think method to fill that in

    cols = ['Race_ID', 'Place', 'Horse_Number', 'Horse_ID', 'Weight', 'Weight_Declared', 'Draw', 'Distance_Between',
            'Place_Section_1', 'Place_Section_2', 'Place_Section_3', 'Place_Section_4', 'Finish_time', 'Jockey_ID',
            'Trainer_ID']
    df = df[cols].copy()
    return df

def horse_preprocessing(df):
    cols = ['Horse_ID', 'State', 'Country', 'Age', 'Color', 'Sex', 'Import_type', 'Total_Stakes', 'Last_Rating']
    df = df[cols].copy()
    return df

def sectional_preprocessing(df):
    print(df.columns.tolist())
    cols = ['Race_ID', 'Horse_ID', 'Finish_time']
    df = df[cols].copy()
    return df


if __name__ == '__main__':
    df_races, df_races_sectional, df_trainer, df_jockeys, df_records, df_horse, df_sectional = read_data()
    df_trainer = trainer_preprocessing(df_trainer)
    df_jockeys = jockeys_preprocessing(df_jockeys)
    df_races = race_preprocessing(df_races)
    df_records = record_preprocessing(df_records)
    df_horse = horse_preprocessing(df_horse)
    df_sectinoal = sectional_preprocessing(df_sectional)

    #

    # print(df_records.shape[0])
    # print(df_records.info())
    df_records_jockey = df_records.merge(df_jockeys, on='Jockey_ID', how='left')
    # print(df_records_jockey.shape[0])
    # print(df_records_jockey.info())
    df_records_jockey_trainer = df_records_jockey.merge(df_trainer, on='Trainer_ID', how='left')
    # print(df_records_jockey_trainer.shape[0])
    # print(df_records_jockey_trainer.info())
    df_records_jockey_trainer_race = df_records_jockey_trainer.merge(df_races, on='Race_ID', how='left')
    # print(df_records_jockey_trainer_race.shape[0])
    # print(df_records_jockey_trainer_race.info())
    df_records_jockey_trainer_race_horse = df_records_jockey_trainer_race.merge(df_horse, on='Horse_ID', how='left')
    # print(df_records_jockey_trainer_race_horse.shape[0])
    # print(df_records_jockey_trainer_race_horse.info())
    df_records_jockey_trainer_race_horse_sectional = df_records_jockey_trainer_race_horse.merge(df_sectional, left_on=['Horse_ID', 'Race_ID'], right_on=['Horse_ID','Race_ID'], how='left')
    # print(df_records_jockey_trainer_race_horse_sectional.shape[0])
    #
    df_records_jockey_trainer_race_horse_sectional = df_records_jockey_trainer_race_horse_sectional.dropna()

    df_records_jockey_trainer_race_horse_sectional.to_csv('df_records_jockey_trainer_race_horse_sectional.csv')

    print(df_records_jockey_trainer_race_horse_sectional['Date'].head())


    weather_data = pd.read_csv('weather_data.csv')
    print(weather_data['month'])
    weather_data['year'] = weather_data['year'].astype('object')
    weather_data['month'] = weather_data['month'].astype('object')
    weather_data['day'] = weather_data['day'].astype('object')

    weather_data['month'] = weather_data['month'].apply(lambda x : str(x).rjust(2,'0'))
    weather_data['day'] = weather_data['day'].apply(lambda x : str(x).rjust(2,'0'))

    weather_data['Date'] = weather_data.apply(lambda x: str(x['year']) + '-' + str(x['month']) + '-' + str(x['day']), axis=1)
    print(weather_data['Date'])
    print(weather_data.info())
    weather_data = weather_data[['Date', ]]

    # df_records_jockey_trainer_race_horse_sectional_weather


