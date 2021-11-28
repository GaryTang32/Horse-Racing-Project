from dao import weather_data_dao
import pandas as pd

if __name__ == '__main__':
    weather_data = weather_data_dao.get_weather_data()
    weather_data.to_csv('weather_data.csv')
    print(weather_data)
