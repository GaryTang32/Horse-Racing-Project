import pprint
import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup
import os
import subprocess
import stat

MAX_AIR_TAG = ['maximum', 'air']
REL_HUMI_TAG = ['relative', 'humidity']
SHA_TIN_TAG = ['sha', 'tin']
HAPPY_VELLEY_TAG = ['happy', 'valley']
HONG_KONG_PARK_TAG = ['hong', 'kong', 'park']

# needs to be changed
# mongopath = r'C:\Program Files\MongoDB\Server\4.2\bin'
mongopath = r'/usr/bin'

def get_weather_data():
    weather_data = pd.DataFrame(
        {'year': [], 'month': [], 'day': [], 'humidity': [], 'sha_tin_min': [], 'sha_tin_max': [], 'happy_velley_min': [],
         'happy_velley_max': []})

    day1 = datetime.date(2015, 1, 1)
    day2 = datetime.date(2019, 9, 9)
    #day2 = datetime.date(2008, 1, 7)
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

    day1 = datetime.date(2019, 9, 12)
    day2 = datetime.date(2021, 10, 30)
    #day2 = datetime.date(2019, 9, 16)
    #day2 = datetime.date(2021, 10, 30)

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

weather_data = get_weather_data()

print(weather_data)
#Added by Pasindu
mongoscript = ""
data_dictionary = dict(weather_data)
for row_idx in range(weather_data.shape[0]):
    mongoscript += f'{{"year":{data_dictionary["year"][row_idx]},"month":{data_dictionary["month"][row_idx]},"day":{data_dictionary["day"][row_idx]},"humidity":{data_dictionary["humidity"][row_idx]},"sha_tin_min":{data_dictionary["sha_tin_min"][row_idx]},"sha_tin_max":{data_dictionary["sha_tin_max"][row_idx]},"happy_velley_min":{data_dictionary["happy_velley_min"][row_idx]},"happy_velley_max":{data_dictionary["happy_velley_max"][row_idx]}}}'
    if row_idx != weather_data.shape[0] - 1:
        mongoscript = mongoscript + ","

mongoscript = "db.WeatherData.insertMany([" + mongoscript + "])"
mongoshellcmd = [mongoscript]

with open('weatherdata_script.txt','w') as f:
    for cmd in mongoshellcmd:
        f.writelines(cmd)
os.chmod(os.path.join(os.getcwd(),"weatherdata_script.txt"),stat.S_IRWXU | stat.S_IRWXO | stat.S_IRWXG )
mongocmd = f'mongo HorseRacing "{os.path.join(os.getcwd(),"weatherdata_script.txt")}"'

#cmd_list = [f'cd {mongopath}',mongocmd]
#cmd_list = ['echo "Came Here"']

cmd_list = [mongocmd]
with open('weatherdata_script.bat','w') as f:
    for cmd in cmd_list:
        f.writelines(cmd)
os.chmod(os.path.join(os.getcwd(),"weatherdata_script.bat"),stat.S_IRWXU | stat.S_IRWXO | stat.S_IRWXG )

#output = subprocess.call([os.path.join(os.getcwd(),'weatherdata_script.bat')])
subprocess.run(mongocmd,shell = True)
#print('Uploaded successfully' if output == 0 else "Upload unsuccessful")

