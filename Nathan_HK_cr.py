# Nathan HK
# 2024-11-30

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
import time

"""
PEP-8 compliant.
"""

pause = 1.5  # 1.5 when running, higher when testing

try:
    df = pd.read_csv('symbol_sample.csv')
except FileNotFoundError:
    df = pd.read_parquet('symbol_sample.parquet')
    df.to_csv('symbol_sample.csv')


def getMSNID(df):
    """
    Scrape the MSN.com IDs of stocks.
    """
    if 'msn_id' in df.columns:
        return
    msn_id = []
    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.get('https://www.msn.com/en-us/money')
    old_url = 'https://www.msn.com/en-us/money'
    time.sleep(pause)
    for i in range(df.shape[0]):
        try:
            if i % 100 == 0:
                print(i)
            leit = driver.find_element(By.XPATH,
                                       '//div[@id="searchBox"]/input')
            leit.send_keys(df.loc[i, 'symbol'])
            time.sleep(pause)
            leit.send_keys(Keys.RETURN)
            time.sleep(pause)
            if old_url == driver.current_url:
                msn_id.append('')
                leit.clear()
                continue
            old_url = driver.current_url
            ticker = driver.find_element(By.XPATH, '//div[@id="fdc_header"]'
                                         '//span[@class="symbolWithBtn-'
                                         'DS-EntryPoint1-1"]').text
            if ticker == df.loc[i, 'symbol']:
                try:
                    msn_id.append(driver.current_url.split('?id=')[1])
                except IndexError:
                    msn_id.append('')
            time.sleep(pause)
        except NoSuchElementException:
            print('nse', i)
            msn_id.append('!ERROR')
            driver.save_screenshot('driver_nse_' + str(i) + '.png')
            time.sleep(pause)
        except StaleElementReferenceException:
            print('ser', i)
            msn_id.append('!ERROR')
            driver.save_screenshot('driver_ser_' + str(i) + '.png')
            time.sleep(pause)
    driver.quit()
    df['msn_id'] = msn_id
    df.to_csv('symbol_sample.csv')


if __name__ == '__main__':
    getMSNID(df)
