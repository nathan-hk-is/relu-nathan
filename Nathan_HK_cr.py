# Nathan HK
# 2024-11-30

import geopy
import numpy as np
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.common.exceptions import NoSuchAttributeException
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException
import sys
import time

"""
Command-line arguments:
1. Name of function to run

PEP-8 compliant.
"""

pause = 2  # 2 when running, higher when testing

try:
    df = pd.read_csv('symbol_sample.csv', index_col=0)
except FileNotFoundError:
    df = pd.read_parquet('symbol_sample2.parquet')
    df.to_csv('symbol_sample.csv')


def getMSNID(df):
    """
    Scrape the MSN.com IDs of stocks.

    Only works for NASDAQ, NYSE, and PNK.
    """
    msn_id = []
    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.get('https://www.msn.com/en-us/money')
    old_url = 'https://www.msn.com/en-us/money'
    time.sleep(pause)
    i = 0
    while i < df.shape[0]:
        try:
            if i % 100 == 0:
                print(i)
            leit = driver.find_element(By.XPATH,
                                       '//div[@id="searchBox"]/input')
            leit.clear()
            leit.send_keys(df.loc[i, 'symbol'])
            time.sleep(pause)
            leit.send_keys(Keys.RETURN)
            time.sleep(pause)
            if old_url == driver.current_url:
                msn_id.append('')
                i += 1
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
            else:
                msn_id.append('')
            time.sleep(pause)
            i += 1
        except NoSuchElementException:
            print('nse', i)
            time.sleep(pause)
        except StaleElementReferenceException:
            print('ser', i)
            time.sleep(pause)
    driver.quit()
    df['msn_id'] = msn_id
    df.to_csv('symbol_sample.csv')


def isOnYahoo(df):
    """
    Find whether each company is on Yahoo! Finance.
    """
    on_yahoo = []
    driver = webdriver.Chrome()
    driver.maximize_window()
    for i in range(df.shape[0]):
        if i % 100 == 0:
            print(i)
        driver.get('https://finance.yahoo.com/lookup/?s=' +
                   df.loc[i, 'symbol'])
        time.sleep(pause)
        try:
            a = driver.find_element(By.XPATH, '//table[@data-testid='
                                    '"table-container"]//a[@href="'
                                    '/quote/' + df.loc[i, 'symbol'] + '/"]')
            on_yahoo.append(True)
        except NoSuchElementException:
            on_yahoo.append(False)
        if 'on_yahoo' in df.columns and on_yahoo[-1] != df.loc[i, 'on_yahoo']:
            print('UPDATE', i, df.loc[i, 'symbol'])
    driver.quit()
    df['on_yahoo'] = on_yahoo
    df.to_csv('symbol_sample.csv')


def getProfileInfo(df):
    """
    Get the street address and website URL of each company.
    """
    websites = []
    addresses = []
    lat = []
    lon = []
    driver = webdriver.Chrome()
    driver.maximize_window()
    geolocator = geopy.geocoders.Nominatim(user_agent='my_map')
    i = 0
    while i < df.shape[0]:
        if i % 100 == 0:
            print(i)
        if not df.loc[i, 'on_yahoo']:
            websites.append('')
            addresses.append('')
            lat.append('')
            lon.append('')
            i += 1
            continue
        driver.get('https://finance.yahoo.com/quote/' + df.loc[i, 'symbol'] +
                   '/profile/')
        time.sleep(pause)
        try:
            data = driver.find_element(By.XPATH, '//section[@data-testid='
                                       '"asset-profile"]')
        except NoSuchElementException:
            websites.append('')
            addresses.append('')
            lat.append('')
            lon.append('')
            driver.delete_all_cookies()
            driver.get('https://finance.yahoo.com/')
            time.sleep(pause)
            i += 1
            continue
        try:
            url = data.find_element(By.XPATH, './/a[@aria-label='
                                    '"website link"]').text
            websites.append(url)
        except NoSuchElementException:
            websites.append('')
        try:
            adiv = data.find_elements(By.XPATH, './/div[@class="address '
                                      'yf-wxp4ja"]//div')
            hfang = ''  # Write to CSV
            hfang_l = ''  # Look up lat/lon
            for a in adiv:
                hfang += a.text + ', '
                if a.text[:6] == 'Suite ':
                    continue
                elif a.text[:5] == 'Unit ':
                    continue
                if a.text[:6] == 'Level ':
                    continue
                hfang_l += a.text + ', '
            hfang = hfang[:-2]
            hfang_l = hfang_l[:-2]
            addresses.append(hfang)
            try:
                hnit = geolocator.geocode(hfang_l)
                if hnit is None:
                    lat.append('')
                    lon.append('')
                else:
                    lat.append(hnit.latitude)
                    lon.append(hnit.longitude)
            except geopy.exc.GeocoderUnavailable:
                lat.append('')
                lon.append('')
        except NoSuchElementException:
            addresses.append('')
            lat.append('')
            lon.append('')
        i += 1
    df['website_yahoo'] = websites
    df['address_yahoo'] = addresses
    df['lat'] = lat
    df['lon'] = lon
    df.to_csv('symbol_sample.csv')


def dataByCountry(df):
    """
    Get data from individual countries' websites.

    Countries where it works:
    - US: https://www.sec.gov/search-filings
        - works to get IDs
        - forms not tested

    Countries in progress:
    - BR: https://dados.gov.br/home

    Countries where it doesn't work:
    - AU: https://connectonline.asic.gov.au/RegistrySearch/
        - can't verify ticker
        - documents behind paywall
    - CA: https://www.sedarplus.ca/landingpage/
        - bot-proof
        - documents are PDF only
    - SE: https://finanscentralen.fi.se/search/search.aspx
        - can't verify ticker
        - filings are all in different formats, chosen by company

    Countries where I can't find the website with filings:
    - CH
    - DE
    - DK
    - HK
    - JP
    - KR
    - PL
    """
    gov_id = {'us': []}
    driver = webdriver.Chrome()
    driver.maximize_window()

    # US
    driver.get('https://www.sec.gov/search-filings')
    leit = driver.find_element(By.XPATH,
                               '//input[@id="edgar-company-person"]')
    for i in range(df.shape[0]):
        if i % 100 == 0:
            print(i)
        if df.loc[i, 'exchange_short_name'] in ['AMEX', 'NASDAQ', 'NYSE']:
            leit.clear()
            leit.send_keys(df.loc[i, 'symbol'])
            time.sleep(pause)
            with open('tempf.html', 'w') as outfile:
                outfile.write(driver.page_source)
            xp = '//table[@class="smart-search-entity-hints"]//a'
            links = driver.find_elements(By.XPATH, xp)
            f = False
            for a in links:
                if '(' in a.text and a.text[-1] == ')':
                    ticks = a.text[:-1].split('(')[1].split(', ')
                    if df.loc[i, 'symbol'] in ticks:
                        hr = a.get_attribute('href')
                        gov_id['us'].append(hr.split('&')[0].
                                            split('?CIK=')[1])
                        f = True
                        break
            if not f:
                gov_id['us'].append('')
            leit.clear()
            time.sleep(pause)
        else:
            gov_id['us'].append('')
    for a in gov_id:
        df['gov_id_' + a] = gov_id[a]
    df.to_csv('symbol_sample.csv')


def findNewsPage(df):
    byrjun = time.time()
    news_pages = []
    driver = webdriver.Chrome()
    driver.maximize_window()
    for i in range(df.shape[0]):
        if i % 100 == 0:
            print(i, time.time() - byrjun)
        if type(df.loc[i, 'website']) is not str:  # No web address
            news_pages.append('')
            continue
        if len(driver.window_handles) > 1:
            driver.quit()
            driver = webdriver.Chrome()
            driver.maximize_window()
        if df.loc[i, 'website'][-1] == '/':
            slash = ''
        else:
            slash = '/'
        for a in ['news', 'press', 'newsroom', 'company-news', 'media']:
            turl = df.loc[i, 'website'] + slash + a
            # Get static HTML page
            try:
                svar = requests.get(turl, timeout=pause * 10)
            except requests.exceptions.SSLError:
                print('SSL ERROR', i, df.loc[i, 'symbol'])
                break
            except requests.exceptions.Timeout:
                print('TIMEOUT', i, df.loc[i, 'symbol'])
                break
            except requests.exceptions.ConnectionError:
                print('CONNECTION ERROR', i, df.loc[i, 'symbol'])
                break
            except requests.exceptions.MissingSchema:
                print('MISSING SCHEMA', i, df.loc[i, 'symbol'])
                news_pages.append('')
                break
            except requests.exceptions.TooManyRedirects:
                print('REDIRECTS', i, df.loc[i, 'symbol'])
                break
            except requests.exceptions.ChunkedEncodingError:
                print('CHUNK', i, df.loc[i, 'symbol'])
                break
            # Find true URL
            if str(svar.status_code)[0] in '123':  # Success or redirect
                news_pages.append(svar.url)
                break
            elif str(svar.status_code)[0] == '4':  # Client error
                continue
            elif str(svar.status_code)[0] == '5':  # Server error
                print('SERVER ERROR', i, df.loc[i, 'symbol'])
                news_pages.append('')
                break
        if len(news_pages) > i:
            continue
        url_list = [df.loc[i, 'website'] + slash + 'ekki_gilt_url',  # 404
                    df.loc[i, 'website']]  # Home page
        for url in url_list:
            if url != '':
                try:
                    driver.get(url)
                    time.sleep(pause)
                    old_url = driver.current_url
                except WebDriverException:
                    print('WDE', i, df.loc[i, 'symbol'])
                    news_pages.append('')
                    break
            # Accept cookies
            bts = driver.find_elements(By.XPATH, '//button')
            cookie_text = ['accept all', 'accept all cookies',
                           'accept cookies', 'i accept', 'allow cookies',
                           'allow all cookies']
            for b in bts:
                if b.text.strip().lower() in cookie_text:
                    try:
                        b.click()
                        time.sleep(pause)
                        break
                    except ElementClickInterceptedException:
                        continue
            # Find links
            alist = driver.find_elements(By.XPATH, '//a')
            news_text = ['news', 'press', 'newsroom', 'press room',
                         'company news', 'press releases', 'news & media',
                         'news and media', 'media releases', 'media',
                         'news and events', 'news & events']
            for a in alist:
                try:
                    if a.text.strip().lower() in news_text:
                        try:
                            a.click()
                            time.sleep(pause)
                        except ElementClickInterceptedException:
                            news_pages.append('')
                            break
                        if driver.current_url != old_url:
                            news_pages.append(driver.current_url)
                            break
                except StaleElementReferenceException:
                    continue
            if len(news_pages) > i:
                break
            if url == df.loc[i, 'website']:
                try:
                    lang = driver.find_element(By.XPATH, '/html'). \
                           get_attribute('lang')
                    if lang == 'en' or lang[:3] == 'en-':
                        break
                    else:
                        f = False
                        for a in driver.find_elements(By.XPATH, '//a'):
                            if a.text.strip().lower() in ['eng', 'english']:
                                try:
                                    a.click()
                                    time.sleep(pause)
                                    f = True
                                    url_list.append('')
                                    break
                                except ElementClickInterceptedException:
                                    continue
                        if not f:
                            url_list.append(df.loc[i, 'website'] + slash +
                                            'en')
                except NoSuchAttributeException:
                    url_list.append(df.loc[i, 'website'] + slash + 'en')
                except NoSuchElementException:
                    url_list.append(df.loc[i, 'website'] + slash + 'en')
        if len(news_pages) == i:
            news_pages.append('')
    df['news_page'] = news_pages
    df.to_csv('symbol_sample.csv')
    print(time.time() - byrjun)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('ERROR! Must include function name.')
    elif sys.argv[1] == 'isOnYahoo':
        isOnYahoo(df)
    elif sys.argv[1] == 'getProfileInfo':
        getProfileInfo(df)
    elif sys.argv[1] == 'dataByCountry':
        dataByCountry(df)
    elif sys.argv[1] == 'findNewsPage':
        findNewsPage(df)
    else:
        print('ERROR! Invalid function name.')
