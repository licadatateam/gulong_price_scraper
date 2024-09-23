# -*- coding: utf-8 -*-
"""
Created on Sat Apr 13 23:28:57 2024

@author: carlo
"""

import requests
import json
import re
import numpy as np
import pandas as pd
import time
from datetime import datetime as dt

import cleaner_functions
import get_chromedriver

from selenium.webdriver.common.by import By

from base_logger import logger

# https://curlconverter.com/python/
# copy as cURL (bash)

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    'origin': 'https://gogulong.ph',
    'referer': 'https://gogulong.ph/',
    'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
    'x-firebase-appcheck': 'eyJraWQiOiJYcEhKU0EiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxOjY5OTk2NTcyNzM3Mzp3ZWI6YjgxMjg2N2Q0ODJiZTVkYjNhYjhjNCIsImF1ZCI6WyJwcm9qZWN0c1wvNjk5OTY1NzI3MzczIiwicHJvamVjdHNcL2dvZ3Vsb25nIl0sInByb3ZpZGVyIjoicmVjYXB0Y2hhX3YzIiwiaXNzIjoiaHR0cHM6XC9cL2ZpcmViYXNlYXBwY2hlY2suZ29vZ2xlYXBpcy5jb21cLzY5OTk2NTcyNzM3MyIsImV4cCI6MTcxMzEwNDUzMywiaWF0IjoxNzEzMDE4MTMzLCJqdGkiOiJTd1VFdkpOUFhLQzNWMWRBaExTb2FPalVFNm5zbkJuSC1qSFExelNiSncwIn0.TNJYCDHAr6bZxTQtgheOVxkDo4I8t5KHymk0q9iOsqnDYFU2H_6qybjN75BqMiQWfPX7p6FZB3wI_zanijNCLaDKwxevx54FMwMKLWLjHoqKi9Yv4_kgar9jSE0kHIKrGsKrUxP9MGK8ODTWOVv2xzpnGUFG7yIU-7oATTazuYOYAfEB7I-MeOvEq3JoOuQuYW06-SSIBEdX2DD4642NOAR4B6e89vbiJPA1DHN0HgYJWi1x68rGjhIt6WqizhPuoz7M6c-H92otmFSNpXHkHUia5uYyxV8lWqIDjaJxY5h6CAUULvfyM82UNY34k08FP810QULyITN1BJwLDhK_xZA08hkDIA6lj50ojSfZwW-KJ1KEokB678yh8XHndBXhiOOH895TtDjWZLhKNv0yGFQ8CO-g-uH-VsbuYRiy0zSYRJwY1tAabhs7-59EVzz7A3x9Om1RJefUaH04kArPFRqfKzQfJ8afA8y9lZsF9YfbicIZCQGuLjkHZhmW_OoY',
}

xpath_prod = {
                'tires': '//div[@class="text-subtitle-1 font-weight-bold"]',
                'price': '//span[@class="text-h6 secondary--text"]',
                'info': '//div[@class="text-subtitle-2"]',
                'brand': "//div[@class='pl-4 pr-2 px-sm-2 col-sm-12 col-8']",
              }

def get_data(w : str, 
             ar : str, 
             d : str, headers):
    
    json_data = {
        'data': {
            'action': 'search-inventory-for-customer',
            'payload': {
                'width': w,
                'aspectRatio': ar,
                'rimDiameter': d,
            },
            'module': 'virtual_inventory',
        },
    }

    response = requests.post('https://asia-east2-gogulong.cloudfunctions.net/searchRequestLg', 
                         headers=headers, 
                         json=json_data)
    data = json.loads(response.content)
    return data

# Note: json_data will not be serialized by requests
# exactly as it was in the original request.
#data = '{"data":{"action":"search-inventory-for-customer","payload":{"width":"175","aspectRatio":"65","rimDiameter":"14"},"module":"virtual_inventory"}}'
#response = requests.post('https://asia-east2-gogulong.cloudfunctions.net/searchRequestLg', headers=headers, data=data)

def gogulong_scraper_network(df_gulong : None):
    # 1. filter out unnecessary specs to be scraped
    correct_specs = [cs for cs in np.sort(
        df_gulong.loc[:, 'correct_specs'].unique()) if float(cs.split('/')[0]) > 27]
    
    # 2. iteration loop
    gg_df_list = []
    for n, spec in enumerate(correct_specs):
        w, ar, d = spec.split('/')
        logger.info(f'Extracting GoGulong info with tire size: {spec}')
        
        try:
            prod_list = get_data(w, ar, d, headers)['result']['result']
            tire_list = []
            tire_cols = ['tire_id', 'size', 'slug',
                         'plyRating', 'width', 'aspectRatio', 'rimDiameter']
            
            for p in prod_list:
                temp_dict = {}
                for col in tire_cols:
                    if col in p['tire'].keys():
                        temp_dict[col] = p['tire'][col]
                    else:
                        temp_dict[col] = None
                    
                temp_dict['sellingPrice'] = p['sellingPrice']
                temp_dict['tireBrand'] = p['tire']['tireDesign']['tireBrand']
                temp_dict['designName'] = p['tire']['tireDesign']['designName']
                tire_list.append(temp_dict)
                
            gg_df_list.append(pd.DataFrame(tire_list))   
            time.sleep(np.random.randint(1, 5))
        except:
            continue
    
    gg_df = pd.concat(gg_df_list, ignore_index = True)
    
    return gg_df


def scrape_data(driver, 
                xpath_info : dict) -> pd.DataFrame:
    '''

    Parameters
    ----------
    driver : selenium
        chrome driver
    xpath_prod : dictionary
        Dictionary of tires, price, info html xpaths separated by website
    site : string, optional
        Which site is being scraped. The default is 'gulong'.

    Returns
    -------
    list
        list of lists containing text of scraped info (tire, price, info)

    '''
    df_dict = {}
    # collect different data on available tires in driver
    for col in ['tires', 'price', 'info']:
        temp = driver.find_elements(By.XPATH, xpath_info[col])
        df_dict[col] = [t.text for t in temp if t.text != '']

    # extract brand
    try:
        brand_gulong = driver.find_elements(By.XPATH, xpath_info['brand'])
        brands_list = []
        for b in brand_gulong:
            try:
                logo_link = b.find_elements(By.XPATH, './/*')[0].get_attribute('src')
                brand = re.search('(?<=img/).*(?=-logo)', logo_link)[0].upper()
                brands_list.append(brand)
            except:
                brand = b.text.split('\n')[0].upper()
                brands_list.append(brand)
    except:
        brands_list = [None] * len(df_dict['tires'])
    
    finally:
        df_dict['brand'] = brands_list
        
    # construct dataframe
    results_df = pd.DataFrame(df_dict)
    
    return results_df

def gogulong_scraper_selenium(driver, 
                     xpath_prod : dict, 
                     df_gulong : pd.DataFrame) -> dict:
    '''
    Gogulong price scraper
    
    Parameters
    ----------
    driver : selenium
        Chrome driver
    xpath_prod : dictionary
        Dictionary of tires, price, info html xpaths separated by website
    df_gulong: dataframe
        Dataframe of scraped data from gulong

    Returns
    -------
        - gg_df_dict : dict
            dictionary containing scraped tire info per tire size
    '''
    
    # 1. filter out unnecessary specs to be scraped
    correct_specs = [cs for cs in np.sort(
        df_gulong.loc[:, 'correct_specs'].unique()) if float(cs.split('/')[0]) > 27]
    
    # 2. iteration loop
    gg_df_dict = {}
    for n, spec in enumerate(correct_specs):
        try:
            # 2.1 obtain specs
            w, ar, d = spec.split('/')
            logger.info(f'Extracting GoGulong info with tire size: {spec}')
            
            # 2.2 open web page
            url_page = 'https://gogulong.ph/search-results?width=' + \
                w + '&aspectRatio=' + ar + '&rimDiameter=' + d
            
            driver.get(url_page)
            
            # 2.3 check error message
            # check if error message for page
            err_message = len(driver.find_elements(
                By.XPATH, '//div[@class="searchResultEmptyMessage"]'))
            
            if err_message == 0:
                driver.implicitly_wait(np.random.randint(1, 5))
                # get number of items
                nums = driver.find_elements(By.XPATH, '//span[@class="grey--text"]')
                num_items = sum([int(n.text[1]) for n in nums])
                logger.debug('{} items on this page: '.format(num_items))
                
                # scrape data
                gg_df_dict[spec] = scrape_data(driver, 
                                               xpath_prod) 
        
        # page-level exception
        except Exception as e:
            raise e
        
        finally:
            total = len(pd.concat(gg_df_dict))
            if (n >= 15) and (total == 0):
                raise Exception('Scraper unable to collect data from gogulong. Terminating scraper.')
                break
            else:
                logger.info('Collected total {} tire items'.format(total))
    
    gg_df = pd.concat(gg_df_dict, axis=0).reset_index(drop = True)
    
    return gg_df

def construct_gogulong_df(df_gogulong : pd.DataFrame, 
                          df_gulong : pd.DataFrame) -> pd.DataFrame:
    
    '''
    
    Parameters
    ----------
        - df_gogulong : pd.DataFrame
            dataframe containing scraped tire info
        - df_gulong : pd.DataFrame
            imported gulong dataframe
    
    Returns
    -------
        - df_gogulong : pd.DataFrame
            cleaned dataframe of scraped gogulong tire info
    
    '''
    
    # rename columns if needed
    df_gogulong = df_gogulong.rename(columns = {'tireBrand' : 'brand',
                                  'plyRating' : 'ply',
                                  'aspectRatio' : 'aspect_ratio',
                                  'rimDiameter' : 'diameter',
                                  'sellingPrice' : 'price_gogulong'})
    # model
    try:
        df_gogulong.loc[:,'name'] = df_gogulong.apply(lambda x: cleaner_functions.fix_names(x['designName'], 
                                                                comp = df_gulong[df_gulong.name.notna()].name.unique()), axis=1)
    except:
        df_gogulong.loc[:,'name'] = df_gogulong.apply(lambda x: cleaner_functions.fix_names(x['tires'], 
                                                                comp = df_gulong[df_gulong.name.notna()].name.unique()), axis=1)
    
    df_gogulong.loc[:,'width'] = df_gogulong.loc[:,'info'].apply(lambda x: cleaner_functions.clean_width(re.search("(\d{3}/)|(\d{2}[Xx])|(\d{3} )", x)[0][:-1]))
    df_gogulong.loc[:,'aspect_ratio'] = df_gogulong.loc[:, 'info'].apply(lambda x: cleaner_functions.clean_aspect_ratio(re.search("(/\d{2})|(X.{4})|( R)", x)[0][1:]))
    df_gogulong.loc[:,'diameter'] = df_gogulong.loc[:, 'info'].apply(lambda x: cleaner_functions.clean_diameter(re.search('R.*\d{2}', x)[0].replace(' ', '')[1:3]))
    df_gogulong.loc[:,'ply'] = df_gogulong.loc[:,'info'].apply(lambda x: re.search('(\d{1}PR)|(\d{2}PR)', x)[0][:-2] if re.search('(\d{1}PR)|(\d{2}PR)', x) else '0')
    df_gogulong.loc[:,'price_gogulong'] = df_gogulong.loc[:,'price'].apply(lambda x: float((x.split(' ')[1]).replace(',', '')))
    df_gogulong.loc[:, 'raw_specs'] = df_gogulong.apply(lambda x: cleaner_functions.combine_specs(str(x['width']), str(x['aspect_ratio']), str(x['diameter']), mode = 'SKU'), axis=1)
    df_gogulong.loc[:, 'correct_specs'] = df_gogulong.apply(lambda x: cleaner_functions.combine_specs(x['width'], x['aspect_ratio'], x['diameter'], mode = 'MATCH'), axis=1)
    df_gogulong.loc[:, 'sku_name'] = df_gogulong.apply(lambda x: cleaner_functions.combine_sku(str(x['brand']), 
                                                           str(x['width']),
                                                           str(x['aspect_ratio']),
                                                           str(x['diameter']),
                                                           str(x['name']), 
                                                           np.NaN, 
                                                           np.NaN), 
                                                           axis=1)
    df_gogulong.reset_index(inplace = True, drop = True)
    cols = ['sku_name', 'brand', 'name', 'raw_specs', 'width', 'aspect_ratio',
            'diameter', 'price_gogulong', 'correct_specs', 'ply']
    
    return df_gogulong[cols]

def main(df_ref : None):
    '''
    
    Parameters:
    ----------
        - df_ref : pd.DataFrame
            df_gulong from get_gulong_data function. Required.
    
    Returns:
    --------
        - dict
    
    '''
    
    time_start = dt.now()
    
    try:
        # try first scraper using requests
        logger.info('Scraping via network.')
        gg_df = gogulong_scraper_network(df_ref)
        if (len(gg_df) == 0) or (gg_df is None):
            raise Exception
    
    except:
        # resort to selenium scraper method
        logger.info('Resorting to Selenium scraper.')
        driver = get_chromedriver.create_driver()
        gg_df = gogulong_scraper_selenium(driver,
                                          xpath_prod,
                                          df_ref)
    
    df_gogulong = construct_gogulong_df(gg_df, 
                                        df_ref)
    
    time_finish = dt.now()
    
    return {'source' : 'gogulong',
            'df' : df_gogulong,
            'items': len(df_gogulong),
            'time_start': time_start.strftime('%Y-%m-%d %H:%M:%S'),
            'time_end': time_finish.strftime('%Y-%m-%d %H:%M:%S'),
            'duration': f'{(time_finish-time_start).seconds} secs',
            }

def main_test(df_ref : None):
    
    time_start = dt.now()
    
    spec = '175/55/15'
    w, ar, d = spec.split('/')
    
    
    # 2.2 open web page
    driver = get_chromedriver.create_driver()
    url_page = 'https://gogulong.ph/search-results?width=' + \
        w + '&aspectRatio=' + ar + '&rimDiameter=' + d
    
    driver.get(url_page)
    
    # 2.3 check error message
    # check if error message for page
    err_message = len(driver.find_elements(
        By.XPATH, '//div[@class="searchResultEmptyMessage"]'))
    
    gg_df_dict = {}
    if err_message == 0:
        driver.implicitly_wait(np.random.randint(1, 5))
        # get number of items
        nums = driver.find_elements(By.XPATH, '//span[@class="grey--text"]')
        num_items = sum([int(n.text[1]) for n in nums])
        logger.debug('{} items on this page: '.format(num_items))
        
        # scrape data
        gg_df_dict[spec] = scrape_data(driver, 
                                       xpath_prod) 
    
    gg_df = pd.concat(gg_df_dict, axis=0).reset_index(drop = True)
    
    df_gogulong_test = construct_gogulong_df(gg_df, df_ref)
    
    time_finish = dt.now()
    
    driver.quit()
    
    return {'source' : 'gogulong',
            'df' : df_gogulong_test,
            'items': len(df_gogulong_test),
            'time_start': time_start.strftime('%Y-%m-%d %H:%M:%S'),
            'time_end': time_finish.strftime('%Y-%m-%d %H:%M:%S'),
            'duration': f'{(time_finish-time_start).seconds} secs',
            }
    