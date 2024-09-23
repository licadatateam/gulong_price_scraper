# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 21:01:50 2024

@author: carlo
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime as dt

# custom modules
import cleaner_functions
import get_chromedriver

# selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from base_logger import logger

def tiremanila_scraper(driver, 
                       df_gulong : pd.DataFrame) -> dict:
    '''
    TireManila price scraper
    
    Parameters:
    ----------
    driver : selenium
        Chrome driver
    df_gulong : pd.DataFrame
        Dataframe of gulong ph data

    Returns:
    -------
    tm_df_dict : dict
        dictionary of scraped raw data from tiremanila
    '''
    # 1. Extract number of pages
    try:
        url_page = 'https://tiremanila.com/?page=1'
        driver.get(url_page)
        
        wait = WebDriverWait(driver, timeout = 5)
        wait.until(EC.presence_of_element_located((By.XPATH, '//a[@tabindex="0"]')))
        pages = driver.find_elements(By.XPATH, '//a[@tabindex="0"]')
        last_page = max([int(page.text) for page in pages if page.text.isnumeric()])
    
    except:
        last_page = 104
    
    
    tm_df_dict = {}
    col_dict = {'Index:' : 'load_index',
                'Style:': 'style',
                'Qty:': 'qty'}
    for page in range(1, last_page+1):
        
        try:
            url_page = 'https://tiremanila.com/?page=' + str(page)
            driver.get(url_page)
            logger.info("Extracting tires info from Tiremanila page: {}".format(page))
            
            # optional
            driver.implicitly_wait(2)
            wait = WebDriverWait(driver, 
                                 timeout = 5, 
                                 poll_frequency = 0.2)
            wait.until(EC.presence_of_all_elements_located((By.XPATH, 
                                            '//div[@class="sv-tile sv-list-view sv-size-big"]')))
            
            products = driver.find_elements(By.XPATH, 
                                            '//div[@class="sv-tile sv-list-view sv-size-big"]')
        
        except:
            continue
        
        if len(products):
            for ndx, p in enumerate(products):
                try:
                    prod_dict = {}
                    prod_dict['name'] = p.find_elements(By.XPATH, '//h3[@class="sv-tile__title sv-text-reset sv-link-reset"]')[ndx].text
                    prod_dict['info'] = p.find_elements(By.XPATH, '//div[@class="sv-badge-list"]')[ndx].text
                    prod_dict['price'] = p.find_elements(By.XPATH, '//p[@class="sv-tile__price sv-text-reset"]')[ndx].text
                    temp = driver.find_elements(By.XPATH, '//div[@class="sv-tile__table sv-no-border"]')
                    
                    for j in temp:
                        split_info = j.text.split('\n')
                        for index, i in enumerate(list(col_dict.keys())):
                            if i in split_info:
                                prod_dict[col_dict[i]] = split_info[split_info.index(i)+1]
                            else:
                                prod_dict[col_dict[i]] = None
                    
                    tm_df_dict[prod_dict['name']] = prod_dict
                except:
                    continue

    return tm_df_dict

def get_tire_info(row):
    '''
    Helper function to extract tire information 
    terrain, on_stock, year
    Used by tiremanila_scraper
    
    Parameters:
    -----------
        - row : dataframe row
    
    Returns:
        tuple : on_stock status, year, terrain info
    '''

    on_stock, year, terrain = None, None, None
    info = row.split('\n')
    
    for i in info:
        # status info
        if i in ['On Stock', 'Pre-Order']:
            on_stock = i
        
        # year info
        elif i.isnumeric():
            year = i
        
        # terrain info
        else:
            terrain = i

    return terrain, on_stock, year

def get_specs(raw_specs):
    '''
    Helper function to extract dimensions from raw specs of tiremanila products
    '''
    
    diam_slice = raw_specs.split('R')
    diameter = diam_slice[1]
   
    if '/' in diam_slice[0]:
        temp = diam_slice[0].split('/')
        return temp[0], temp[1], diameter
    
    elif 'X' in diam_slice[0]:
        temp = diam_slice[0].split('X')
        return temp[0], temp[1], diameter
    
    else:
        return diam_slice[0], 'R', diameter

def get_brand_model(sku_name):
    '''
    Helper function to extract brand and model from tiremanila products
    
    Parameters
    ----------
    sku_name: str
        sku_name row from dataframe
        
    Returns
    -------
    brand: string
        
    model: string
    '''
    sku_minus_specs = sku_name.upper().split(' ')[1:]
    if '(' in sku_minus_specs[0]:
        sku_minus_specs = sku_minus_specs[1:]
    
    brand_dict = {'BFG\s': 'BFGOODRICH ',
                  'DOUBLE COIN' : 'DOUBLECOIN',
                  '8PR' : 'MICHELIN',
                  '10PR' : 'MICHELIN'}
    
    sku_minus_specs = ' '.join(sku_minus_specs)
    for key in brand_dict.keys():
        if re.search(key, sku_minus_specs):
            sku_minus_specs = re.sub(key, brand_dict[key], sku_minus_specs)
        else:
            continue
    
    sku_minus_specs = sku_minus_specs.split(' ')
    brand = sku_minus_specs[0]
    model = ' '.join(sku_minus_specs[1:]).strip()
    return brand, model

def construct_tiremanila_df(tm_df_dict, df_gulong):
    '''
    
    Parameters
    ----------
        - tm_df_dict : dict
            dictionary of dataframes containing scraped tire info with specs as keys
        - df_gulong : pd.DataFrame
            imported gulong dataframe
    
    Returns
    -------
        - df_tiremanila : pd.DataFrame
            cleaned dataframe of scraped gogulong tire info
    
    '''
    
    
    df_tiremanila = pd.DataFrame(tm_df_dict).T.reset_index(drop = True)
    df_tiremanila = df_tiremanila.rename(columns = {'name' : 'sku_name',
                                                    'qty' : 'qty_tiremanila'})
    
    try:

        df_tiremanila = df_tiremanila[(df_tiremanila.sku_name != '') & (df_tiremanila.price != '')]
        df_tiremanila['terrain'], df_tiremanila['on_stock'], df_tiremanila['year'] = zip(*df_tiremanila['info'].map(get_tire_info))
        df_tiremanila['year'] = df_tiremanila['year'].apply(cleaner_functions.clean_year)
        df_tiremanila.loc[:, 'price_tiremanila'] = df_tiremanila.apply(lambda x: round(float(''.join(x['price'][1:].split(','))), 2), axis=1)
        df_tiremanila.loc[:, 'raw_specs'] = df_tiremanila.apply(lambda x: x['sku_name'].split(' ')[0], axis=1)
        df_tiremanila['width'], df_tiremanila['aspect_ratio'], df_tiremanila['diameter'] = zip(*df_tiremanila.loc[:, 'raw_specs'].map(get_specs))
        df_tiremanila['brand'], df_tiremanila['model'] = zip(*df_tiremanila.loc[:, 'sku_name'].map(get_brand_model))
        df_tiremanila.loc[:,'name'] = df_tiremanila.apply(lambda x: cleaner_functions.fix_names(x['model'], 
                                                                                            comp = df_gulong[df_gulong.name.notna()].name.unique()), 
                                                                                              axis=1)
        df_tiremanila.loc[:,'width'] = df_tiremanila.apply(lambda x: cleaner_functions.clean_width(x['width'], model = x['name']), 
                                                           axis=1)
        df_tiremanila.loc[:,'aspect_ratio'] = df_tiremanila.apply(lambda x: cleaner_functions.clean_aspect_ratio(x['aspect_ratio'], 
                                                                                                                 model = x['name']), 
                                                                  axis=1)
        df_tiremanila.loc[:,'diameter'] = df_tiremanila.apply(lambda x: cleaner_functions.clean_diameter(x['diameter']), axis=1)
        df_tiremanila.loc[:, 'raw_specs'] = df_tiremanila.apply(lambda x: cleaner_functions.combine_specs(str(x['width']), 
                                                                                                          str(x['aspect_ratio']), 
                                                                                                          str(x['diameter']), mode = 'SKU'), 
                                                                axis=1)
        df_tiremanila.loc[:, 'correct_specs'] = df_tiremanila.apply(lambda x: cleaner_functions.combine_specs(x['width'], 
                                                                                                              x['aspect_ratio'], 
                                                                                                              x['diameter'], 
                                                                                                              mode = 'MATCH'), 
                                                                    axis=1)
        df_tiremanila.loc[:, 'sku_name'] = df_tiremanila.apply(lambda x: cleaner_functions.combine_sku(str(x['brand']),
                                                                                     str(x['width']),
                                                                                     str(x['aspect_ratio']),
                                                                                     str(x['diameter']),
                                                                                     str(x['name']),
                                                                                     np.NaN,
                                                                                     np.NaN), axis=1)
        df_tiremanila.drop(labels='info', axis=1, inplace=True)
        # reorder cols
        df_tiremanila = df_tiremanila[['sku_name', 'name', 'model', 'brand', 
                                'price_tiremanila', 'qty_tiremanila', 'year', 
                                'width', 'aspect_ratio', 'diameter', 'raw_specs', 
                                'correct_specs']]
        
    except Exception as e:
        raise e
    
    return df_tiremanila

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
    
    # 1. Create chromedriver instance
    driver = get_chromedriver.create_driver()
    
    time_start = dt.now()
    # 2. Scrape raw data
    tm_df_dict = tiremanila_scraper(driver, 
                                    df_ref)
    
    # 3. Construct cleaned dataframe
    df_tiremanila = construct_tiremanila_df(tm_df_dict, 
                                            df_ref)
    time_finish = dt.now()
    
    # 4. Close chromedriver
    driver.quit()
    
    # 5. Return scraping results and stats
    return {'source': 'tiremanila',
            'df' : df_tiremanila,
            'items': len(df_tiremanila),
            'time_start': time_start.strftime('%Y-%m-%d %H:%M:%S'),
            'time_end': time_finish.strftime('%Y-%m-%d %H:%M:%S'),
            'duration': f'{(time_finish-time_start).seconds} secs',
            }

def main_test(df_ref = None):
    
    time_start = dt.now()
    
    tm_df_dict = {}
    col_dict = {'Index:' : 'load_index',
                'Style:': 'style',
                'Qty:': 'qty'}
    
    driver = get_chromedriver.create_driver()
    page = 1
    url_page = f'https://tiremanila.com/?page={page}'
    driver.get(url_page)
    logger.info("Extracting tires info from Tiremanila page: {}".format(page))
    
    # optional
    wait = WebDriverWait(driver, timeout = 5, poll_frequency = 0.2)
    wait.until(EC.presence_of_all_elements_located((By.XPATH, 
                                    '//div[@class="sv-tile sv-list-view sv-size-big"]')))
    
    products = driver.find_elements(By.XPATH, 
                                    '//div[@class="sv-tile sv-list-view sv-size-big"]')
    
    if len(products):
        for ndx, p in enumerate(products):
            prod_dict = {}
            prod_dict['name'] = p.find_elements(By.XPATH, '//h3[@class="sv-tile__title sv-text-reset sv-link-reset"]')[ndx].text
            prod_dict['info'] = p.find_elements(By.XPATH, '//div[@class="sv-badge-list"]')[ndx].text
            prod_dict['price'] = p.find_elements(By.XPATH, '//p[@class="sv-tile__price sv-text-reset"]')[ndx].text
            temp = driver.find_elements(By.XPATH, '//div[@class="sv-tile__table sv-no-border"]')
            
            for j in temp:
                split_info = j.text.split('\n')
                for index, i in enumerate(list(col_dict.keys())):
                    if i in split_info:
                        prod_dict[col_dict[i]] = split_info[split_info.index(i)+1]
                    else:
                        prod_dict[col_dict[i]] = None
            
            tm_df_dict[prod_dict['name']] = prod_dict
    
    df_tiremanila_test = construct_tiremanila_df(tm_df_dict, 
                                                   df_ref)
    
    time_finish = dt.now()
    # 4. Close chromedriver
    driver.quit()
    
    # 5. Return scraping results and stats
    return {'source': 'tiremanila',
            'df' : df_tiremanila_test,
            'items': len(df_tiremanila_test),
            'time_start': time_start.strftime('%Y-%m-%d %H:%M:%S'),
            'time_end': time_finish.strftime('%Y-%m-%d %H:%M:%S'),
            'duration': f'{(time_finish-time_start).seconds} secs',
            }