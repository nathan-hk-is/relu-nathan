# Nathan HK
# 2024-11-30

import bs4
from bs4 import BeautifulSoup
from datetime import datetime
import dateutil
import geopy
import json
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
    """
    For a company, find the "news" or "press releases" page on the website.

    Assumptions:
    - HTML "lang" parameter is always correct.
    - All hrefs on a website are valid.
    """
    byrjun = time.time()
    news_pages = []
    driver = webdriver.Chrome()
    driver.maximize_window()
    findtype = [0, 0, 0]
    timetype = [0.0, 0.0, 0.0]
    for i in range(df.shape[0]):
        if i % 100 == 0:
            print(i, time.time() - byrjun, findtype, timetype)
        if type(df.loc[i, 'website']) is not str:  # No web address
            news_pages.append('')
            continue
        tegbyr = time.time()
        if len(driver.window_handles) > 1:
            driver.quit()
            driver = webdriver.Chrome()
            driver.maximize_window()
        if df.loc[i, 'website'][-1] == '/':
            slash = ''
        else:
            slash = '/'
        skipRBS = False
        url_list = [df.loc[i, 'website']]  # Home page
        news_text = ['news', 'press', 'newsroom', 'press room',
                     'company news', 'press releases', 'news & media',
                     'news and media', 'media releases', 'media',
                     'news and events', 'news & events']
        # Python requests / BeautifulSoup
        for url in url_list:
            if skipRBS:
                break
            try:
                svar = requests.get(url, timeout=pause * 10)
            except requests.exceptions.SSLError:
                print('SSL ERROR A', i, df.loc[i, 'symbol'])
                skipRBS = True
                break
            except requests.exceptions.Timeout:
                print('TIMEOUT A', i, df.loc[i, 'symbol'])
                skipRBS = True
                break
            except requests.exceptions.ConnectionError:
                print('CONNECTION ERROR A', i, df.loc[i, 'symbol'])
                skipRBS = True
                break
            except requests.exceptions.MissingSchema:
                print('MISSING SCHEMA A', i, df.loc[i, 'symbol'])
                skipRBS = True
                news_pages.append('')
                break
            except requests.exceptions.TooManyRedirects:
                print('REDIRECTS A', i, df.loc[i, 'symbol'])
                break
            except requests.exceptions.ChunkedEncodingError:
                print('CHUNK A', i, df.loc[i, 'symbol'])
                break
            # Find true URL
            if str(svar.status_code)[0] in '123':  # Success or redirect
                webpage = BeautifulSoup(svar.content, 'html.parser')
                en_needed = False
                if url == df.loc[i, 'website']:
                    try:
                        lang = webpage.find('html')['lang']
                        if lang == 'en' or lang[:3] == 'en-':
                            pass
                        else:
                            url_list.append(df.loc[i, 'website'] + slash +
                                            'en')
                            en_needed = True
                    except AttributeError:
                        url_list.append(df.loc[i, 'website'] + slash + 'en')
                    except KeyError:
                        url_list.append(df.loc[i, 'website'] + slash + 'en')
                    except TypeError:
                        url_list.append(df.loc[i, 'website'] + slash + 'en')
                alist = webpage.find_all('a')
                for a in alist:
                    try:
                        if a.getText().strip().lower() in news_text:
                            if a['href'][:8] == 'https://':
                                news_pages.append(a['href'])
                            elif a['href'][0] == '/':
                                news_pages.append('https://' + svar.url.
                                                  split('/')[2] + a['href'])
                            findtype[0] += 1
                            break
                        elif en_needed and (a.getText().strip().lower() in
                                            ['eng', 'english']):
                            if a['href'][:8] == 'https://':
                                url_list[1] = a['href']
                            elif a['href'][0] == '/':
                                url_list[1] = ('https://' + svar.url.
                                               split('/')[2] + a['href'])
                            en_needed = False
                    except AttributeError:
                        continue
                    except KeyError:  # no href
                        continue
                    except IndexError:  # href is empty string
                        continue
                if len(news_pages) > i:
                    break
            else:  # Client or server error
                continue
        timetype[0] += time.time() - tegbyr
        if len(news_pages) > i:
            continue
        # Common URLs
        tegbyr = time.time()
        for a in ['news', 'press', 'newsroom', 'company-news', 'media',
                  'press-releases', 'news-press-releases']:
            if skipRBS:
                break
            turl = df.loc[i, 'website'] + slash + a
            # Get static HTML page
            try:
                svar = requests.get(turl, timeout=pause * 10)
            except requests.exceptions.SSLError:
                print('SSL ERROR B', i, df.loc[i, 'symbol'])
                break
            except requests.exceptions.Timeout:
                print('TIMEOUT B', i, df.loc[i, 'symbol'])
                break
            except requests.exceptions.ConnectionError:
                print('CONNECTION ERROR B', i, df.loc[i, 'symbol'])
                break
            except requests.exceptions.MissingSchema:
                print('MISSING SCHEMA B', i, df.loc[i, 'symbol'])
                news_pages.append('')
                break
            except requests.exceptions.TooManyRedirects:
                print('REDIRECTS B', i, df.loc[i, 'symbol'])
                break
            except requests.exceptions.ChunkedEncodingError:
                print('CHUNK B', i, df.loc[i, 'symbol'])
                break
            # Find true URL
            if str(svar.status_code)[0] in '123':  # Success or redirect
                news_pages.append(svar.url)
                findtype[1] += 1
                break
            else:  # Client or server error
                continue
        timetype[1] += time.time() - tegbyr
        if len(news_pages) > i:
            continue
        # Selenium
        tegbyr = time.time()
        url_list = [df.loc[i, 'website']]  # Home page
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
                           'allow all cookies', 'i accept cookies']
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
            for a in alist:
                try:
                    if a.text.strip().lower() in news_text:
                        href = a.get_attribute('href')
                        if href[:8] == 'https://':
                            news_pages.append(href)
                        elif href[0] == '/':
                            news_pages.append('https://' + svar.url.
                                              split('/')[2] + href)
                        findtype[2] += 1
                        break
                except NoSuchAttributeException:
                    continue
                except StaleElementReferenceException:
                    continue
                except TypeError:
                    continue
                except IndexError:
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
        timetype[2] += time.time() - tegbyr
    df['news_page'] = news_pages
    df.to_csv('symbol_sample.csv')
    print(time.time() - byrjun)


def getReportsPage(df):
    byrjun = time.time()
    driver = webdriver.Chrome()
    driver.maximize_window()
    reports_pages = []
    rpby = {}
    rphr_s = ['annual report', 'integrated report', 'financial report',
              'annual results', 'financial results', 'corporate report',
              'company report', 'financial statements']
    rphr_p = ['annual reports', 'integrated reports', 'financial reports',
              'reports - ', 'reports | ', 'corporate reports',
              'company reports', 'financial statement']
    for i in range(df.shape[0]):
        if i % 100 == 0:
            print(i)
        if type(df.loc[i, 'website']) is not str:
            reports_pages.append('')
            continue
        year_range = range(int(df.loc[i, 'min_date'][:4]),
                           int(df.loc[i, 'max_date'][:4]) + 1)
        for y in year_range:
            try:
                rpby[y]
            except KeyError:
                rpby[y] = {}
        if df.loc[i, 'website'][:8] == 'https://':
            web_by = df.loc[i, 'website'].split('/')[2]
        else:
            web_by = df.loc[i, 'website']
        if web_by[:4] == 'www.':
            web_by = web_by[4:]
        driver.get('https://duckduckgo.com/?q=annual+financial+report+site:' +
                   web_by)
        time.sleep(pause / 10)
        xp = '//ol[@class="react-results--main"]/li[@data-layout="organic"]'
        # Wait to load
        while True:
            results = driver.find_elements(By.XPATH, xp)
            if len(results) > 0:
                break
            try:
                par = driver.find_element(By.XPATH, '//p[@class='
                                          '"w7syQmNN6Yjvw6guGJuQ"]')
                if 'No results found' in par.text:
                    break
            except NoSuchElementException:
                pass
            time.sleep(pause / 10)
        for wpage in results:
            try:
                a = wpage.find_element(By.XPATH, './/h2/a')
                href = a.get_attribute('href')
            except NoSuchAttributeException:
                continue
            except NoSuchElementException:
                continue
            for p in rphr_p:
                if len(reports_pages) > i:
                    break
                if p in a.text.lower():
                    reports_pages.append(href)
                    break
            for y in year_range:
                try:
                    rpby[y][i]
                    continue
                except KeyError:
                    pass
                for p in rphr_s:
                    if str(y) + ' ' + p in a.text.lower() or \
                       p + ' ' + str(y) in a.text.lower():
                        rpby[y][i] = href
                        break
                if 'year ' + str(y) + ' results' in a.text.lower():
                    rpby[y][i] = href
            try:
                snip = wpage.find_element(By.XPATH, './/div[@data-result='
                                          '"snippet"]').text.split('.')[0]
                for y in year_range:
                    try:
                        rpby[y][i]
                        continue
                    except KeyError:
                        pass
                    for p in rphr_s:
                        if str(y) + ' ' + p == snip.lower() or \
                           p + ' ' + str(y) == snip.lower():
                            rpby[y][i] = href
                            break
                    if 'year ' + str(y) + ' results' in a.text.lower():
                        rpby[y][i] = href
            except NoSuchElementException:
                continue
            mod_href = href.lower().replace('%20', ' ').\
                replace('_', ' ').replace('-', ' ')
            for p in rphr_p:
                if len(reports_pages) > i:
                    break
                if p in mod_href:
                    reports_pages.append(href)
                    break
            for y in year_range:
                try:
                    rpby[y][i]
                    continue
                except KeyError:
                    pass
                for p in rphr_s:
                    if str(y) + ' ' + p in mod_href or \
                       p + ' ' + str(y) in mod_href:
                        rpby[y][i] = href
                        break
                if 'year ' + str(y) + ' results' in a.text.lower():
                    rpby[y][i] = href
        if len(reports_pages) == i:
            reports_pages.append('')
    df['reports_page'] = reports_pages
    for year in range(min(rpby), max(rpby) + 1):
        try:
            rpby[year]
        except KeyError:
            rpby[year] = {}
        by_temp = []
        for i in range(df.shape[0]):
            try:
                by_temp.append(rpby[year][i])
            except KeyError:
                by_temp.append('')
        df['report_' + str(year)] = by_temp
    has_any = []
    for i in range(df.shape[0]):
        f = False
        if type(df.loc[i, 'news_page']) is str and \
           df.loc[i, 'news_page'] != '':
            has_any.append(True)
            continue
        if type(df.loc[i, 'reports_page']) is str and \
           df.loc[i, 'reports_page'] != '':
            has_any.append(True)
            continue
        for y in range(min(rpby), max(rpby) + 1):
            if type(df.loc[i, 'report_' + str(y)]) is str and \
               df.loc[i, 'report_' + str(y)] != '':
                f = True
                break
        has_any.append(f)
    df['any_subweb'] = has_any
    df.to_csv('symbol_sample.csv')
    print(time.time() - byrjun)


def oneNewsArt(href, i, df):
    """
    Parse an individual news article.
    """
    news_ind = {'url': href}
    try:
        nyttsvar = requests.get(href, timeout=pause * 10)
    except requests.exceptions.SSLError:
        print('SSL ERROR B', i, df.loc[i, 'symbol'])
        return None
    except requests.exceptions.Timeout:
        print('TIMEOUT B', i, df.loc[i, 'symbol'])
        return None
    except requests.exceptions.ConnectionError:
        print('CONNECTION ERROR B', i, df.loc[i, 'symbol'])
        return None
    except requests.exceptions.MissingSchema:
        print('MISSING SCHEMA B', i, df.loc[i, 'symbol'])
        return None
    except requests.exceptions.TooManyRedirects:
        print('REDIRECTS B', i, df.loc[i, 'symbol'])
        return None
    except requests.exceptions.ChunkedEncodingError:
        print('CHUNK B', i, df.loc[i, 'symbol'])
        return None
    except requests.exceptions.InvalidSchema:
        print('INVALID SCHEMA B', i, df.loc[i, 'symbol'])
        return None
    except requests.exceptions.InvalidURL:
        print('INVALID URL B', i, df.loc[i, 'symbol'])
        return None
    if nyttsvar.status_code >= 400:
        print('STATUS CODE B', i, df.loc[i, 'symbol'])
        return None
    try:
        one_art = BeautifulSoup(nyttsvar.content, 'html.parser')
    except bs4.builder.ParserRejectedMarkup:
        print('PRM B', i, df.loc[i, 'symbol'])
        return None
    except AssertionError:
        print('ASSERT B', i, df.loc[i, 'symbol'])
        return None
    main = one_art.find('div', {'id': 'main'})
    if main is None:
        main = one_art.find('div', {'class': 'main'})
    if main is None:
        main = one_art.find('div', {'class': 'pageContent'})
    div_sim = ['time', 'div', 'span', 'h1', 'h2', 'h3', 'h4', 'h5']
    divlist = []
    for tag in div_sim:
        divlist += one_art.find_all(tag)
    parsed = None
    for div in divlist:
        try:
            if df.loc[i, 'country'] in ['US', 'CA']:
                parsed = dateutil.parser.\
                    parse(div['datetime'], dayfirst=False)
            else:
                parsed = dateutil.parser.\
                    parse(div['datetime'], dayfirst=True)
            break
        except KeyError:
            pass
        except dateutil.parser.ParserError:
            continue
        except dateutil.parser._parser.ParserError:
            continue
        except ValueError:
            continue
        try:
            div['class']
        except KeyError:
            continue
        for cl in div['class']:
            if 'date' in cl.lower():
                try:
                    stripped = div.getText().strip()
                    if df.loc[i, 'country'] in ['US', 'CA']:
                        parsed = dateutil.parser.\
                            parse(stripped, dayfirst=False)
                    else:
                        parsed = dateutil.parser.\
                            parse(stripped, dayfirst=True)
                except dateutil.parser.ParserError:
                    continue
                except dateutil.parser._parser.ParserError:
                    continue
                except ValueError:
                    continue
                break
        if parsed is not None:
            break
    if parsed is None:
        return None
    news_ind['date'] = parsed.date().isoformat()
    if main is None:
        ps = one_art.find_all('p')
    else:
        ps = main.find_all('p')
    art_text = ''
    for p in ps:
        art_text += p.text
        art_text = art_text.strip()
        art_text += ' '
    news_ind['text'] = art_text.strip()
    return news_ind


def readNews(df):
    """
    Given a URL to a list of company news reports ("news_page"), extract the
    text and date from each individual report ("ind_rep").

    First, we try and get the main div (excluding headers and footers).

    There are many extraneous links from the news page. Here is how we
    determine the correct ones:
    - Inside an <article> tag
    - news_page URL is a substring of ind_rep URL
    - news_page.class contains string "article"
    - We get the template for each URL: split by slash, exclude final string.
      Then, we see which templates have at least 10 URLs, and get those links.
    """
    byrjun = time.time()
    nj = {}
    for i in range(df.shape[0]):
        if i % 100 == 0:
            print(i)
        if type(df.loc[i, 'news_page']) is not str:
            continue
        try:
            svar = requests.get(df.loc[i, 'news_page'], timeout=pause * 10)
        except requests.exceptions.SSLError:
            print('SSL ERROR A', i, df.loc[i, 'symbol'])
            continue
        except requests.exceptions.Timeout:
            print('TIMEOUT A', i, df.loc[i, 'symbol'])
            continue
        except requests.exceptions.ConnectionError:
            print('CONNECTION ERROR A', i, df.loc[i, 'symbol'])
            continue
        except requests.exceptions.MissingSchema:
            print('MISSING SCHEMA A', i, df.loc[i, 'symbol'])
            continue
        except requests.exceptions.TooManyRedirects:
            print('REDIRECTS A', i, df.loc[i, 'symbol'])
            continue
        except requests.exceptions.ChunkedEncodingError:
            print('CHUNK A', i, df.loc[i, 'symbol'])
            continue
        except requests.exceptions.InvalidSchema:
            print('INVALID SCHEMA A', i, df.loc[i, 'symbol'])
            continue
        except requests.exceptions.InvalidURL:
            print('INVALID URL A', i, df.loc[i, 'symbol'])
            return None
        if svar.status_code >= 400:
            print('STATUS CODE A', i, df.loc[i, 'symbol'])
            continue
        newslist = []
        nl_urls = []
        webpage = BeautifulSoup(svar.content, 'html.parser')
        main = webpage.find('div', {'id': 'main'})
        if main is None:
            main = webpage.find('div', {'class': 'main'})
        if main is None:
            main = webpage.find('div', {'class': 'pageContent'})
        if main is None:
            alist = webpage.find_all('article')
        else:
            alist = main.find_all('article')
        for art in alist:
            try:
                href = a.find('a')['href']
                if ':' not in href:
                    if href[0] == '/':
                        href = 'https://' + svar.url.split('/')[2] + href
                    else:
                        href = 'https://' + svar.url.split('/')[2] + '/' + href
            except AttributeError:
                continue
            except KeyError:
                continue
            except IndexError:
                continue
            except TypeError:
                continue
            if href.split('?')[0] == svar.url:
                continue
            if href in nl_urls:
                continue
            news_ind = oneNewsArt(href, i, df)
            if news_ind is not None:
                newslist.append(news_ind)
                nl_urls.append(news_ind['url'])
        if main is None:
            alist = webpage.find_all('a')
        else:
            alist = main.find_all('a')
        url_snid = {}
        for a in alist:
            try:
                href = a['href']
                if ':' not in href:
                    if href[0] == '/':
                        href = 'https://' + svar.url.split('/')[2] + href
                    else:
                        href = 'https://' + svar.url.split('/')[2] + '/' + href
            except AttributeError:
                continue
            except KeyError:
                continue
            except IndexError:
                continue
            if href.split('?')[0] == svar.url:
                continue
            if href in nl_urls:
                continue
            slash_s = tuple(href.split('/')[:-1])
            try:
                url_snid[slash_s] += 1
            except KeyError:
                url_snid[slash_s] = 1
            anq = svar.url.split('?')[0]
            if href[:len(anq)] == anq or a.text.strip().lower() == 'read more':
                news_ind = oneNewsArt(href, i, df)
                if news_ind is not None:
                    newslist.append(news_ind)
                    nl_urls.append(news_ind['url'])
                    continue
            try:
                for cl in a['class']:
                    if 'article' in cl:
                        news_ind = oneNewsArt(href, i, df)
                        if news_ind is not None:
                            newslist.append(news_ind)
                            nl_urls.append(news_ind['url'])
            except AttributeError:
                continue
            except KeyError:
                continue
        # 10 or more URL templates
        for a in alist:
            try:
                href = a['href']
                if ':' not in href:
                    if href[0] == '/':
                        href = 'https://' + svar.url.split('/')[2] + href
                    else:
                        href = 'https://' + svar.url.split('/')[2] + '/' + href
            except AttributeError:
                continue
            except KeyError:
                continue
            except IndexError:
                continue
            if href.split('?')[0] == svar.url:
                continue
            if href in nl_urls:
                continue
            slash_one = tuple(href.split('/')[:-1])
            if len(slash_one) == 0:
                continue
            if slash_one in url_snid and url_snid[slash_one] >= 10:
                news_ind = oneNewsArt(href, i, df)
                if news_ind is not None:
                    newslist.append(news_ind)
                    nl_urls.append(news_ind['url'])
                    continue
        nj[df.loc[i, 'symbol']] = newslist
    with open('news.json', 'w') as outfile:
        json.dump(nj, outfile, indent=4)
    print('\n= FINISHED =')
    print('Time:', time.time() - byrjun)
    print('Failure rate:', len([a for a in nj if len(nj[a]) == 0]) / len(nj))


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
    elif sys.argv[1] == 'getReportsPage':
        getReportsPage(df)
    elif sys.argv[1] == 'readNews':
        readNews(df)
    else:
        print('ERROR! Invalid function name.')
