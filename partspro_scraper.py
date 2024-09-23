# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 21:57:26 2024

@author: carlo
"""

import pandas as pd
import numpy as np
import re
from fuzzywuzzy import process
from datetime import datetime as dt

# custom modules
import cleaner_functions
import get_chromedriver

# selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from base_logger import logger

def partspro_scraper(driver,
                     df_gulong : pd.DataFrame) -> pd.DataFrame:
    
    '''
    PartsPro product scraper
    
    Parameters:
    ----------
    driver : selenium
        Chrome driver
    df_gulong : pd.DataFrame
        Dataframe of gulong ph data

    Returns:
    -------
        - df : pd.DataFrame
            dataframe of scraped data from partspro
    '''
    
    try:
        url_page = 'https://www.partspro.ph/search?type=product&options%5Bprefix%5D=last&options%5Bunavailable_products%5D=last&q=product_type%3ATires&page=1'
        driver.get(url_page)
        
        wait = WebDriverWait(driver, timeout = 5)
        # driver.implicitly_wait(3)
        wait.until(EC.presence_of_element_located((By.XPATH, '//a[@class="pagination__nav-item link"]')))
        pages = driver.find_elements(By.XPATH, '//a[@class="pagination__nav-item link"]')
        last_page = max([int(page.text) for page in pages if page.text.isnumeric()])
    
    except:
        last_page = 64
    
    prod_list = []
    for page in range(1, last_page+1):
        logger.info(f'Extracting PartsPro info from page: {page}')
        try:
            url = f"https://www.partspro.ph/search?type=product&options%5Bprefix%5D=last&options%5Bunavailable_products%5D=last&q=product_type%3ATires&page={page}"
            driver.get(url)
            
            # extract products texts
            products = [p.text.split('\n') for p in driver.find_elements(By.XPATH, 
                                            '//div[@class="product-item__info-inner"]')]
            
            for p in products:
                try:
                    try:
                        brand = process.extractOne(p[0], 
                                                   df_gulong.brand.unique(), 
                                                   score_cutoff = 90)[0]
                    except:
                        brand = p[0].strip().upper()
                        
                    sku_name = p[1].upper().strip()
                    price = cleaner_functions.clean_price([_ for _ in p if '₱' in _][0])
                    model = cleaner_functions.clean_model(sku_name, df_gulong)
                
                    # cleaned tire specs
                    try:
                        specs = re.sub(f'{brand}|{model}', '', sku_name).strip()
                    except:
                        specs = sku_name
                        
                    width, aspect_ratio, diameter = cleaner_functions.clean_specs(specs)
                    aspect_ratio = cleaner_functions.clean_aspect_ratio(aspect_ratio)
                    diameter = cleaner_functions.clean_diameter(diameter)
                    raw_specs_ = cleaner_functions.combine_specs(width, aspect_ratio, diameter, mode = 'SKU')
                    correct_specs = cleaner_functions.combine_specs(width, aspect_ratio, diameter, mode = 'MATCH')
                    name = cleaner_functions.fix_names(model, 
                                                       comp = df_gulong[df_gulong.name.notna()].name.unique())
                    # extract load and speed index
                    try:                 
                        load_speed = re.search('(?<=R[0-9]{2}\s)[0-9]{2,3}(\/)?([0-9]{2,3})?[A-Z]', sku_name)[0]
                        # find all numbers in searched text
                        load = re.findall('[0-9]{2,3}', load_speed)
                        load_index = '/'.join(load) if len(load) > 1 else load[0]
                        speed_index = re.search('[A-Z]', load_speed)[0]
                        
                    except:
                        load_index = ''
                        speed_index = ''
                    
                    # construct final sku name
                    sku_name = cleaner_functions.combine_sku(brand,
                                                             width,
                                                             aspect_ratio,
                                                             diameter,
                                                             name,
                                                             load_index,
                                                             speed_index)
                    
                    # append data dict to list
                    prod_list.append({
                            'sku_name':sku_name,
                            'price_partspro':price,
                            'brand':brand,
                            'name':name,
                            'width':width,
                            'aspect_ratio':aspect_ratio,
                            'diameter':diameter,
                            'raw_specs':raw_specs_,
                            'correct_specs':correct_specs,
                            'load_index':load_index,
                            'speed_index':speed_index
                            })
                # product level scraping exception
                except:
                    continue
        
        # page-level scraping exception
        except:
            continue
        
        driver.implicitly_wait(np.random.randint(1, 5))
    
    df = pd.DataFrame(prod_list)
    df = df[df.brand != 'PARTSPRO.PH']
    
    return df

def main(df_ref = None):
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
    
    # 2. Scrape data
    df_partspro = partspro_scraper(driver, 
                                   df_ref)
    
    time_finish = dt.now()
    
    # 3. Close chromedriver
    driver.quit()
    
    # 4. Return scraping results and stats
    return {'source' : 'partspro',
            'df' : df_partspro,
            'items': len(df_partspro),
            'time_start': time_start.strftime('%Y-%m-%d %H:%M:%S'),
            'time_end': time_finish.strftime('%Y-%m-%d %H:%M:%S'),
            'duration': f'{(time_finish-time_start).seconds} secs',
            }

def main_test(df_gulong = None):
    
    # 1. Create chromedriver instance
    driver = get_chromedriver.create_driver()
    
    time_start = dt.now()
    
    page = 1
    
    url = f"https://www.partspro.ph/search?type=product&options%5Bprefix%5D=last&options%5Bunavailable_products%5D=last&q=product_type%3ATires&page={page}"
    driver.get(url)
    
    # extract products texts
    products = [p.text.split('\n') for p in driver.find_elements(By.XPATH, 
                                    '//div[@class="product-item__info-inner"]')]
    prod_list = []
    for p in products:
            try:
                brand = process.extractOne(p[0], 
                                           df_gulong.brand.unique(), 
                                           score_cutoff = 90)[0]
            except:
                brand = p[0].strip().upper()
                
            sku_name = p[1].upper().strip()
            price = cleaner_functions.clean_price([_ for _ in p if '₱' in _][0])
            model = cleaner_functions.clean_model(sku_name, df_gulong)
        
            # cleaned tire specs
            try:
                specs = re.sub(f'{brand}|{model}', '', sku_name).strip()
            except:
                specs = sku_name
                
            width, aspect_ratio, diameter = cleaner_functions.clean_specs(specs)
            aspect_ratio = cleaner_functions.clean_aspect_ratio(aspect_ratio)
            diameter = cleaner_functions.clean_diameter(diameter)
            raw_specs_ = cleaner_functions.combine_specs(width, aspect_ratio, diameter, mode = 'SKU')
            correct_specs = cleaner_functions.combine_specs(width, aspect_ratio, diameter, mode = 'MATCH')
            name = cleaner_functions.fix_names(model, 
                                               comp = df_gulong[df_gulong.name.notna()].name.unique())
            # extract load and speed index
            try:                 
                load_speed = re.search('(?<=R[0-9]{2}\s)[0-9]{2,3}(\/)?([0-9]{2,3})?[A-Z]', sku_name)[0]
                # find all numbers in searched text
                load = re.findall('[0-9]{2,3}', load_speed)
                load_index = '/'.join(load) if len(load) > 1 else load[0]
                speed_index = re.search('[A-Z]', load_speed)[0]
                
            except:
                load_index = ''
                speed_index = ''
            
            # construct final sku name
            sku_name = cleaner_functions.combine_sku(brand,
                                                     width,
                                                     aspect_ratio,
                                                     diameter,
                                                     name,
                                                     load_index,
                                                     speed_index)
            
            # append data dict to list
            prod_list.append({
                    'sku_name':sku_name,
                    'price_partspro':price,
                    'brand':brand,
                    'name':name,
                    'width':width,
                    'aspect_ratio':aspect_ratio,
                    'diameter':diameter,
                    'raw_specs':raw_specs_,
                    'correct_specs':correct_specs,
                    'load_index':load_index,
                    'speed_index':speed_index
                    })
    
    df = pd.DataFrame(prod_list)
    df_partspro_test = df[df.brand != 'PARTSPRO.PH']
    
    time_finish = dt.now()
    
    # 3. Close chromedriver
    driver.quit()
    
    # 4. Return scraping results and stats
    return {'source' : 'partspro',
            'df' : df_partspro_test,
            'items': len(df_partspro_test),
            'time_start': time_start.strftime('%Y-%m-%d %H:%M:%S'),
            'time_end': time_finish.strftime('%Y-%m-%d %H:%M:%S'),
            'duration': f'{(time_finish-time_start).seconds} secs',
            }