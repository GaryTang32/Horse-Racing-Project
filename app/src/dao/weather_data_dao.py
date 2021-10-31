import pprint
import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup

MAX_AIR_TAG = ['maximum', 'air']
REL_HUMI_TAG = ['relative', 'humidity']
SHA_TIN_TAG = ['sha', 'tin']
HAPPY_VELLEY_TAG = ['happy', 'valley']
HONG_KONG_PARK_TAG = ['hong', 'kong', 'park']

def get_weather_data():
    weather_data = pd.DataFrame(
        {'year': [], 'month': [], 'day': [], 'humidity': [], 'sha_tin_min': [], 'sha_tin_max': [], 'happy_velley_min': [],
         'happy_velley_max': []})

    day1 = datetime.date(2008, 1, 1)
    day2 = datetime.date(2019, 9, 9)
    # Max date = 20190910


    days = [day1 + datetime.timedelta(days=x) for x in range((day2 - day1).days + 1)]

    for i in days:

        year = i.year
        month = str(i.month).rjust(2, '0')
        day = str(i.day).rjust(2, '0')
        print(f'Date: {year}/{month}/{day}. Crawling weather data from HTML')
        url = f'https://www.hko.gov.hk/wxinfo/dailywx/yeswx/ryese{year}{month}{day}.htm'
        sentence = BeautifulSoup(requests.get(url).text, 'html.parser').find('pre').get_text().lower().replace('\r', '').replace('//', '').replace('c', '').split('\n')

        sentence_token = [x.split() for x in sentence]
        humidity = 0
        sha_tin_min = 0
        sha_tin_max = 0
        happy_valley_min = 0
        happy_valley_max = 0
        for token in sentence_token:

            if all(x in token for x in REL_HUMI_TAG):
                humidity = token[2] + token[3] + token[4]
            elif all(x in token for x in SHA_TIN_TAG):
                sha_tin_min = token[2]
                sha_tin_max = token[3] if len(token) > 3 else token[2]
            elif all(x in token for x in HAPPY_VELLEY_TAG):
                happy_valley_min = token[2]
                happy_valley_max = token[3]
            elif (all(x in token for x in HONG_KONG_PARK_TAG)) and (happy_valley_min == 0):
                happy_valley_min = token[3]
                # if no max temperature, then use the same.
                happy_valley_max = token[4] if len(token) > 4 else token[3]

            data = {'year': str(year),
                    'month': month,
                    'day': day,
                    'humidity': humidity,
                    'sha_tin_min': float(sha_tin_min),
                    'sha_tin_max': float(sha_tin_max),
                    'happy_velley_min': float(happy_valley_min),
                    'happy_velley_max': float(happy_valley_max)}
        weather_data = weather_data.append(data, ignore_index=True)

    day1 = datetime.date(2019, 9, 10)
    day2 = datetime.date(2021, 10, 30)
    # day2 = datetime.date(2021, 10, 30)

    days = [day1 + datetime.timedelta(days=x) for x in range((day2 - day1).days + 1)]
    for i in days:
        year = i.year
        month = str(i.month).rjust(2, '0')
        day = str(i.day).rjust(2, '0')
        print(f'Date: {year}/{month}/{day}. Crawling weather data from JSON')
        url = f'https://www.hko.gov.hk/wxinfo/dailywx/yeswx/DYN_DAT_MINDS_RYES{year}{month}{day}.json?get_param=value'
        weather_dict = requests.get(url).json()['DYN_DAT_MINDS_RYES']

        humidity = weather_dict['HKOReadingsMinRH']['Val_Eng'] + '-' + weather_dict['HKOReadingsMaxRH']['Val_Eng']

        if weather_dict['ShaTinMinTemp']['Val_Eng'] != '':
            sha_tin_min = weather_dict['ShaTinMinTemp']['Val_Eng']
            sha_tin_max = weather_dict['ShaTinMaxTemp']['Val_Eng']
        elif weather_dict['TaiPoMinTemp']['Val_Eng'] != '':
            sha_tin_min = weather_dict['TaiPoMinTemp']['Val_Eng']
            sha_tin_max = weather_dict['TaiPoMaxTemp']['Val_Eng']
        else:
            sha_tin_min = weather_dict['SaiKungMinTemp']['Val_Eng']
            sha_tin_max = weather_dict['SaiKungMaxTemp']['Val_Eng']

        if weather_dict['HappyValleyMinTemp']['Val_Eng'] != '':
            happy_velley_min = weather_dict['HappyValleyMinTemp']['Val_Eng']
            happy_velley_max = weather_dict['HappyValleyMaxTemp']['Val_Eng']
        elif weather_dict['HongKongParkMinTemp']['Val_Eng'] != '':
            happy_velley_min = weather_dict['HongKongParkMinTemp']['Val_Eng']
            happy_velley_max = weather_dict['HongKongParkMaxTemp']['Val_Eng']
        else:
            happy_velley_min = weather_dict['WongChukHangMinTemp']['Val_Eng']
            happy_velley_max = weather_dict['WongChukHangMaxTemp']['Val_Eng']

        data = {'year': str(year),
                'month': month,
                'day': day,
                'humidity': humidity,
                'sha_tin_min': float(sha_tin_min),
                'sha_tin_max': float(sha_tin_max),
                'happy_velley_min': float(happy_velley_min),
                'happy_velley_max': float(happy_velley_max)
                }
        weather_data = weather_data.append(data, ignore_index=True)
    return weather_data
