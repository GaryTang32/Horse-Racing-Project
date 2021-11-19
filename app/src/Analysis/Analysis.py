import pandas as pd
import numpy as np
from sklearn.cluster import KMeans

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def read_data():
    prefix = r'../Dataset/'
    path_horse = prefix + 'horses.csv'
    path_jockeys = prefix + 'jockeys.csv'
    path_races = prefix + 'races.csv'
    path_races_sectional = prefix + 'races_sectional.csv'
    path_trainer = prefix + 'trainer.csv'
    df_races = pd.read_csv(path_races)
    df_races_sectional = pd.read_csv(path_races_sectional)
    df_trainer = pd.read_csv(path_trainer)
    df_jockey = pd.read_csv(path_jockeys)
    return df_races, df_races_sectional, df_trainer, df_jockey


def trainer_analysis(df):
    # for trainer, only calculate its winning ratio
    print(df.info())
    print(df.head())
    df = df.fillna(0)
    print(df.info())
    df['first_place_ratio'] = df['Total_wins'] / df['Total_rides']
    df['second_place_ratio'] = df['Total_second_places'] / df['Total_rides']
    df['third_place_ratio'] = df['Total_third_places'] / df['Total_rides']
    df['place_ratio'] = (df['Total_wins'] + df['Total_second_places'] + df['Total_third_places']) / df['Total_rides']
    df['lose_ratio'] = 1 - df['place_ratio']
    print(df.info())
    print(df.head())
    return df


def jockeys_analysis(df):
    print(df.info())
    print(df.head())
    df['first_place_ratio'] = df['Total_wins'] / df['Total_rides']
    df['second_place_ratio'] = df['Total_second_places'] / df['Total_rides']
    df['third_place_ratio'] = df['Total_third_places'] / df['Total_rides']
    df['place_ratio'] = (df['Total_wins'] + df['Total_second_places'] + df['Total_third_places']) / df['Total_rides']
    df['lose_ratio'] = 1 - df['place_ratio']
    return df


def race_analysis(df):
    print(df.info())
    df['Year'] = df['Date'].apply(lambda x: str(x)[:4])
    df = df[df['Year'] > '2008'].copy()
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
        df[df['Class'] == i] = class_trans_dict[i]
    df['class_rank'] = df['Class'] + df['Ranking']

    # print(set(df['class_rank'].values.tolist()))
    print(set(df['Class'].values.tolist()))


if __name__ == '__main__':
    df_races, df_races_sectional, df_trainer, df_jockeys = read_data()
    # df_trainer = trainer_analysis(df_trainer)
    # df_jockeys = jockeys_analysis(df_jockeys)
    # df_races = race_analysis(df_races)

    # print(df_races)
