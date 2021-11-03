#function to scrape the foal date of all the horses of which the first race date was found

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import urllib.request
from bs4 import BeautifulSoup
import threading

def find_foal_date_1(name_list):
    results = list()
    url = 'https://www.racenet.com.au'
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
    options = webdriver.ChromeOptions()
    options.add_argument("headless")
    options.add_argument('window-size=1200x600')
    options.add_argument(f'user-agent = {user_agent}')
    driver = webdriver.Chrome(executable_path = r'C:\Users\ASUS\Desktop\MSc\MSBD 5003\Project\chromedriver.exe', chrome_options=options)
    driver.get(url)
    for name in name_list:
        print(name)
        try:   
            search_box = driver.find_element_by_name("s")
            search_box.send_keys(name + Keys.RETURN)
            search_result = BeautifulSoup(driver.page_source,"html.parser")
            result_list = search_result.find_all('a',{'class':'search-grid-row'})
            if len(result_list) == 1:
                rest_of_url = search_result.find_all('a',{'class':'search-grid-row'})[0]['href']
                driver.get(url+rest_of_url)
                horse_page = BeautifulSoup(driver.page_source,'html.parser')
                foal_date = horse_page.find_all('div',{'class':'profile-info__item--value'})[5].find_all('span')[0].contents
                results.append((name,foal_date))
                print('Success')
            else:
                results.append((name,"Non Unique Name"))
                print('Non unique')
        except:
            results.append((name,"No Foal Date"))
            print('no date')
    return results

def find_foal_date_2(name_list):
    results = list()
    url = 'https://www.racenet.com.au'
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
    options = webdriver.ChromeOptions()
    options.add_argument("headless")
    options.add_argument('window-size=1200x600')
    options.add_argument(f'user-agent = {user_agent}')
    driver = webdriver.Chrome(executable_path = r'C:\Users\ASUS\Desktop\MSc\MSBD 5003\Project\chromedriver.exe', chrome_options=options)
    driver.get(url)
    for name in name_list:
        print(name)
        try:   
            search_box = driver.find_element_by_name("s")
            search_box.send_keys(name + Keys.RETURN)
            search_result = BeautifulSoup(driver.page_source,"html.parser")
            result_list = search_result.find_all('a',{'class':'search-grid-row'})
            if len(result_list) == 1:
                rest_of_url = search_result.find_all('a',{'class':'search-grid-row'})[0]['href']
                driver.get(url+rest_of_url)
                horse_page = BeautifulSoup(driver.page_source,'html.parser')
                foal_date = horse_page.find_all('div',{'class':'profile-info__item--value'})[5].find_all('span')[0].contents
                results.append((name,foal_date))
                print('Success')
            else:
                results.append((name,"Non Unique Name"))
                print('Non unique')
        except:
            results.append((name,"No Foal Date"))
            print('no date')
    return results

def find_foal_date_3(name_list):
    results = list()
    url = 'https://www.racenet.com.au'
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
    options = webdriver.ChromeOptions()
    options.add_argument("headless")
    options.add_argument('window-size=1200x600')
    options.add_argument(f'user-agent = {user_agent}')
    driver = webdriver.Chrome(executable_path = r'C:\Users\ASUS\Desktop\MSc\MSBD 5003\Project\chromedriver.exe', chrome_options=options)
    driver.get(url)
    for name in name_list:
        print(name)
        try:   
            search_box = driver.find_element_by_name("s")
            search_box.send_keys(name + Keys.RETURN)
            search_result = BeautifulSoup(driver.page_source,"html.parser")
            result_list = search_result.find_all('a',{'class':'search-grid-row'})
            if len(result_list) == 1:
                rest_of_url = search_result.find_all('a',{'class':'search-grid-row'})[0]['href']
                driver.get(url+rest_of_url)
                horse_page = BeautifulSoup(driver.page_source,'html.parser')
                foal_date = horse_page.find_all('div',{'class':'profile-info__item--value'})[5].find_all('span')[0].contents
                results.append((name,foal_date))
                print('Success')
            else:
                results.append((name,"Non Unique Name"))
                print('Non unique')
        except:
            results.append((name,"No Foal Date"))
            print('no date')
    return results

def find_foal_date_4(name_list):
    results = list()
    url = 'https://www.racenet.com.au'
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'
    options = webdriver.ChromeOptions()
    options.add_argument("headless")
    options.add_argument('window-size=1200x600')
    options.add_argument(f'user-agent = {user_agent}')
    driver = webdriver.Chrome(executable_path = r'C:\Users\ASUS\Desktop\MSc\MSBD 5003\Project\chromedriver.exe', chrome_options=options)
    driver.get(url)
    for name in name_list:
        print(name)
        try:   
            search_box = driver.find_element_by_name("s")
            search_box.send_keys(name + Keys.RETURN)
            search_result = BeautifulSoup(driver.page_source,"html.parser")
            result_list = search_result.find_all('a',{'class':'search-grid-row'})
            if len(result_list) == 1:
                rest_of_url = search_result.find_all('a',{'class':'search-grid-row'})[0]['href']
                driver.get(url+rest_of_url)
                horse_page = BeautifulSoup(driver.page_source,'html.parser')
                foal_date = horse_page.find_all('div',{'class':'profile-info__item--value'})[5].find_all('span')[0].contents
                results.append((name,foal_date))
                print('Success')
            else:
                results.append((name,"Non Unique Name"))
                print('Non unique')
        except:
            results.append((name,"No Foal Date"))
            print('no date')
    return results

function_list = [find_foal_date_1,find_foal_date_2,find_foal_date_3,find_foal_date_4]


#printing user agent
a= driver.execute_script("return navigator.userAgent")

#non headless user agent : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36'

#headless user agent : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/95.0.4638.54 Safari/537.36'

horse_list = first_race_date_df["Horse_Name"].tolist()
#create 4 lists
m = int(len(horse_list)/4)
horse_list = [horse_list[i*m:(i+1)*m] for i in range(4)]
final_result = list()

def do_job(i):
    result = function_list[i](horse_list[i])
    final_result.append(result)

threads = []
for i in range(4):
    t = threading.Thread(target = do_job,args = (i,))
    threads += [t]
    t.start()


for t in threads:
    t.join()