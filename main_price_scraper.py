# -*- coding: utf-8 -*-
"""
Created on Thu Feb  8 08:02:59 2024

@author: carlo
"""
# basic modules
import pandas as pd
import numpy as np
#from datetime import timedelta, date
from datetime import datetime as dt
import json
from functools import reduce
import gspread
import time
import re

# custom modules
import gogulong_scraper, tiremanila_scraper, partspro_scraper
import bq_functions, cleaner_functions

from base_logger import logger

def get_gulong_data() -> pd.DataFrame:
    '''
    Get gulong.ph data from backend
    
    Returns
    -------
    df : dataframe
        Gulong.ph product info dataframe
    '''
    show_cols = ['sku_name', 'raw_specs', 'price_gulong', 'name', 'brand', 
                 'width', 'aspect_ratio', 'diameter', 'correct_specs']
    
    try:
        ## 1. Import from redash query api key
        # http://app.redash.licagroup.ph/queries/131
        url1 =  "http://app.redash.licagroup.ph/api/queries/131/results.csv?api_key=FqpOO9ePYQhAXrtdqsXSt2ZahnUZ2XCh3ooFogzY"
        
        df = pd.read_csv(url1, 
                         parse_dates = ['supplier_price_date_updated',
                                        'product_price_date_updated'],
                         date_format = '%m/%d/%y %H:%M')
              
        ## 2. rename columns
        df = df.rename(columns={'model': 'sku_name',
                                'name': 'supplier',
                                'pattern' : 'name',
                                'make' : 'brand',
                                'section_width' : 'width', 
                                'rim_size':'diameter', 
                                'promo' : 'price_gulong'}).reset_index(drop = True)
        
        ## 3. Perform data filtering and cleaning
        df.loc[df['sale_tag']==0, 'price_gulong'] = df.loc[df['sale_tag']==0, 'srp']
        df = df[df.activity == 1]
        df.loc[:, 'width'] = df.apply(lambda x: cleaner_functions.clean_width(x['width']), axis=1)
        df.loc[:, 'aspect_ratio'] = df.apply(lambda x: cleaner_functions.clean_aspect_ratio(x['aspect_ratio']), axis=1)    
        df.loc[:, 'diameter'] = df.apply(lambda x: cleaner_functions.clean_diameter(x['diameter']), axis=1)
        df.loc[:, 'raw_specs'] = df.apply(lambda x: cleaner_functions.combine_specs(x['width'], x['aspect_ratio'], x['diameter'], mode = 'SKU'), axis=1)
        df.loc[:, 'correct_specs'] = df.apply(lambda x: cleaner_functions.combine_specs(x['width'], x['aspect_ratio'], x['diameter'], mode = 'MATCH'), axis=1)
        df.loc[:, 'name'] = df.apply(lambda x: cleaner_functions.fix_names(x['name']), axis=1)
        df.loc[:, 'sku_name'] = df.apply(lambda x: cleaner_functions.combine_sku(str(x['brand']), 
                                                               str(x['width']),
                                                               str(x['aspect_ratio']),
                                                               str(x['diameter']),
                                                               str(x['name']), 
                                                               str(x['load_rating']), 
                                                               str(x['speed_rating'])), 
                                                               axis=1)
        df = df[df.name != '-']
        
    except Exception as e:
        raise e
    
    return df[show_cols]
           

def get_intersection(df_gulong, df_gogulong, df_tiremanila, df_partspro):
    '''
    Parameters
    ----------
    
    df_gulong : dataframe
        Scraped gulong.ph data
    df_gogulong : dataframe
        Scraped gogulong.ph data
    save : bool
        Save file to csv. The default is True.
    
    Returns
    -------
    
    df_merged: pd.DataFrame
    
    '''
    gulong_cols = ['sku_name', 'name', 'brand', 'price_gulong', 'raw_specs', 
                   'correct_specs']
    gogulong_cols = ['sku_name_gg', 'name', 'brand', 'price_gogulong', 
                     'raw_specs_gg', 'correct_specs']
    tiremanila_cols = ['sku_name_tm', 'name', 'brand', 'price_tiremanila', 
                       'qty_tiremanila', 'year', 'raw_specs_tm', 'correct_specs']
    partspro_cols = ['sku_name_pp', 'name',  'brand', 'price_partspro', 
                     'raw_specs_pp', 'correct_specs']
    
    df_gg = df_gogulong.rename(columns = {'sku_name' : 'sku_name_gg',
                                          'raw_specs' : 'raw_specs_gg'})
    df_tm = df_tiremanila.rename(columns = {'sku_name' : 'sku_name_tm',
                                            'raw_specs' : 'raw_specs_tm'})
    df_pp = df_partspro.rename(columns = {'sku_name' : 'sku_name_pp',
                                                'raw_specs' : 'raw_specs_pp'})
    
    # list of dataframes to merge
    dfs = [df_gulong[gulong_cols], df_gg[gogulong_cols], 
           df_tm[tiremanila_cols], df_pp[partspro_cols]]
    df_merged = reduce(lambda left, right: pd.merge(left, right, 
                                                   how='outer', 
                                                   on=['name', 'brand', 
                                                       'correct_specs']), dfs)
    # base products on gulong.ph
    df_merged_ = df_merged[(pd.notna(df_merged['price_gogulong']) | pd.notna(
        df_merged['price_tiremanila']) | pd.notna(df_merged['price_partspro'])) & pd.notna(df_merged['sku_name'])]
    df_merged_ = df_merged_[['sku_name', 'raw_specs', 'price_gulong',
                             'price_gogulong', 'price_partspro', 'price_tiremanila', 
                             'qty_tiremanila', 'year', 'brand', 'name', 'correct_specs']]
    # gulong only products
    df_gulong_only = df_merged[pd.notna(df_merged['price_gulong']) & pd.isna(
        df_merged['price_gogulong']) & pd.isna(df_merged['price_tiremanila']) & pd.isna(df_merged['price_partspro'])]
    
    df_gulong_only_ = df_gulong_only[['sku_name', 'raw_specs', 'price_gulong',
                                      'price_gogulong', 'price_partspro',
                                      'price_tiremanila', 'qty_tiremanila',
                                      'year', 'brand', 'name', 'correct_specs']].rename(columns={
                                                          'sku_name': 'sku_name',
                                                          'raw_specs': 'raw_specs'})
    # tiremanila only
    df_tm_only = df_merged[pd.isna(df_merged['price_gulong']) & pd.notna(
        df_merged['price_tiremanila'])]
    df_tm_only_ = df_tm_only[['sku_name_tm', 'raw_specs_tm', 'price_gulong', 
                              'price_gogulong', 'price_partspro', 'price_tiremanila',
                              'qty_tiremanila', 'year', 'brand', 'name', 'correct_specs']].rename(columns={
                                                          'sku_name_tm': 'sku_name',
                                                          'raw_specs_tm': 'raw_specs'})
    # gogulong only
    df_gg_only = df_merged[pd.isna(df_merged['price_gulong']) & pd.notna(
        df_merged['price_gogulong'])]
    df_gg_only_ = df_gg_only[['sku_name_gg', 'raw_specs_gg', 'price_gulong', 
                              'price_gogulong', 'price_partspro', 'price_tiremanila',
                              'qty_tiremanila', 'year', 'brand', 'name', 'correct_specs']].rename(columns={
                                                          'sku_name_gg': 'sku_name',
                                                          'raw_specs_gg': 'raw_specs'})
     # partspro only

    df_pp_only = df_merged[pd.isna(df_merged['price_gulong']) & pd.notna(
        df_merged['price_partspro'])]
    df_pp_only_ = df_pp_only[['sku_name_pp', 'raw_specs_pp', 'price_gulong', 'price_gogulong', 'price_partspro', 'price_tiremanila',
                              'qty_tiremanila', 'year', 'brand', 'name', 'correct_specs']].rename(columns={
                                                          'sku_name_pp': 'sku_name',
                                                          'raw_specs_pp': 'raw_specs'})
    
    # combine two datasets
    df_ = pd.concat([df_merged_, df_gulong_only_, df_tm_only_, 
                     df_gg_only_, df_pp_only_], axis=0).sort_values('raw_specs')
    
    # remove duplicates and cleaning
    df_ = df_.drop_duplicates().reset_index(drop = True)
    # remove '' values
    df_ = df_.replace(to_replace = '', value = np.NaN)
    # insert date
    df_['date'] = dt.today().date().strftime('%Y-%m-%d')
    # reorder columns
    df_final = df_[['date', 'raw_specs', 'sku_name', 'price_gulong', 'price_gogulong',
                        'price_partspro', 'price_tiremanila', 'qty_tiremanila',
                        'year', 'brand', 'name', 'correct_specs']]
    
    return df_final

def write_to_gsheet(df):
    '''
    Creates new sheet in designated googlesheet and writes selected data from df
    
    Parameters
    ----------
    df: dataframe
        dataframe to write to google sheet
    
    '''

    try:
        with open('credentials.json') as creds:
            credentials = json.load(creds)
        
        gsheet_key = "12jCVn8EQyxXC3UuQyiRjeKsA88YsFUuVUD3_5PILA2c"
        gc = gspread.service_account_from_dict(credentials)
        sh = gc.open_by_key(gsheet_key)
        
        new_sheet_name = dt.strftime(dt.today(),"%Y/%m/%d")
        r,c = df.shape
        
        try:
            sh.add_worksheet(title=new_sheet_name,rows = r+1, cols = c+1)
            worksheet = sh.worksheet(new_sheet_name)
        except:
            worksheet = sh.worksheet(new_sheet_name)
            worksheet.clear()
        worksheet.update([df.columns.tolist()]+df.values.tolist())
        
    except:
        df.to_csv('gulong_competitor_data.csv')

def upload_gsheet_to_bq(df_ref = None):
    '''
    df_ref = df_gulong
    '''
    with open('credentials.json') as creds:
        credentials = json.load(creds)
    
    gsheet_key = "12jCVn8EQyxXC3UuQyiRjeKsA88YsFUuVUD3_5PILA2c"
    gc = gspread.service_account_from_dict(credentials)
    sh = gc.open_by_key(gsheet_key)
    
    sheets = sh.worksheets()
    df_list = []
    for s in sheets[::-1]:
        try:
            print (s.title)
            working_sheet = sh.worksheet(s.title)
            temp_df = pd.DataFrame(working_sheet.get_all_records())
            temp_df['date'] = pd.to_datetime(s.title).date().strftime('%Y-%m-%d')
            df_list.append(temp_df[~temp_df.brand.isin(['10PR', '8PR'])])
            time.sleep(1)
        except:
            continue
    df_all = pd.concat(df_list, ignore_index = True)
    
    # TODO: clean
    df_all_ = df_all[df_all.qty_tiremanila.apply(lambda x: str(x).isnumeric() or pd.isna(x) or (x == ''))]
    df_all_ = df_all_[df_all_.sku_name != '']
    # clean BGF brands and names
    df_all_.loc[df_all_.brand == 'BFGOODRICHKO2', 'name'] = 'KO2'
    df_all_.loc[df_all_.brand == 'BFGOODRICHKM3', 'name'] = 'KM3'
    df_all_.loc[df_all_.brand == 'BFGOODRICHTRAIL-TERRAIN', 'name'] = 'TRAIL-TERRAIN T/A'
    df_all_.loc[df_all_.brand.apply(lambda x: re.search('BFGOODRICH.+', x) is not None), 'brand'] = 'BFGOODRICH'
    # clean SKU
    df_all_.loc[df_all_.brand.apply(lambda x: re.search('BFGOODRICH.+', x) is not None), 'sku_name'] = df_all_.loc[df_all_.brand.apply(lambda x: re.search('BFGOODRICH.+', x) is not None), :].apply(lambda x: re.sub('BFG', 'BFGOODRICH', x['sku_name']), axis=1)
    
    df_all_['name'] = df_all_.apply(lambda x: cleaner_functions.fix_names(x['name'], df_ref), axis=1)
    df_all_.loc[df_all_.raw_specs == '265/6/R18', 'raw_specs'] = '265/60/R18'
    df_all_['width'], df_all_['aspect_ratio'], df_all_['diameter'] = zip(*df_all_.loc[:, 'raw_specs'].map(cleaner_functions.clean_tire_size))
    df_all_.loc[:, 'raw_specs'] = df_all_.apply(lambda x: cleaner_functions.combine_specs(str(x['width']), str(x['aspect_ratio']), str(x['diameter']), mode = 'SKU'), axis=1)
    df_all_.loc[:, 'correct_specs'] = df_all_.apply(lambda x: cleaner_functions.combine_specs(x['width'], x['aspect_ratio'], x['diameter'], mode = 'MATCH'), axis=1)
    #df_all_['sku_name'] = df_all_.apply(lambda x: re.sub('  ', ' ',' '.join([x['brand'], re.sub('  ', ' ', re.sub(x['brand'], '', x['sku_name']))])).strip().upper(), axis=1)
    df_all_['sku_name'] = df_all_.apply(lambda x: cleaner_functions.combine_sku(str(x['brand']),
                                                                                 str(x['width']),
                                                                                 str(x['aspect_ratio']),
                                                                                 str(x['diameter']),
                                                                                 str(x['name']),
                                                                                 np.NaN,
                                                                                 np.NaN), axis=1)
    
    df_all_ = df_all_.replace(to_replace = '', value = np.NaN)
    df_all_ = df_all_[df_all_.price_tiremanila != 'COOPER 305/70R16 STT PRO'].reset_index(drop = True)
    df_final = df_all_[['date', 'raw_specs', 'sku_name', 'price_gulong', 'price_gogulong',
                        'price_partspro', 'price_tiremanila', 'qty_tiremanila',
                        'year', 'brand', 'name',
                        'correct_specs']]
    
    return df_final

def init_bq(table_name : str = 'gulong_wheel_size') -> dict:
    '''
    Loads BigQuery project info
    
    Returns
    -------
        - bq_dict : dict
            keys contain client, credentials, and project id
    
    '''
    
    # load account object from json file
    acct = bq_functions.get_acct()
    # get client and creds via authentication of acct
    client, credentials = bq_functions.authenticate_bq(acct)
    # check table
    project_id = 'absolute-gantry-363408'
    dataset = 'gulong'
    table_name = 'gulong_wheel_size' if table_name is None else table_name
    table_id = '.'.join([project_id, dataset, table_name])
    
    return {'client' : client,
            'credentials' : credentials,
            'table_id' : table_id}
    
        
def load_save_data(bq_dict : dict,
                   df : pd.DataFrame = None,
                   ls : str = 'load',
                   mode : str = 'WRITE_APPEND'):
    '''
    Load/Save pandas dataframe to csv
    
    Args:
    -----
        - bq_dict : dict
            keys containing client, credentials, project and table id
        - df : pd.DataFrame, default is None
            dataframe to save
        - ls : str, default 
        - mode : str
            'a' for append, 'w' for truncation
    
    '''
    
    if ls == 'load':
        try:
            output = bq_functions.query_bq(bq_dict['table_id'], 
                                           bq_dict['client'])
        except Exception as e:
            output = pd.DataFrame()
        
        finally:
            return output
        
    elif ls == 'save':
        dataset_name, table_name = bq_dict['table_id'].split('.')[1:]
        job = bq_functions.write_bq(client = bq_dict['client'], 
                                  credentials = bq_dict['credentials'], 
                                  table_id = bq_dict['table_id'],
                                  data = df,
                                  write_mode = mode
                                  )
        return None
    
    # df.to_csv('gulong_wheel_size.csv', index = False, mode = mode,
    #           header = False)
    else:
        return None

def main(save : bool = True,
         platform : str = 'all'):
    
    time_start = dt.now()
    ## 1. Import gulong backend data
    df_gulong = get_gulong_data()
    gulong_time = dt.now()
    
    logger.info('Starting GoGulong Scraper.')
    ## 2. Gogulong scraper 
    gogulong_dict = gogulong_scraper.main(df_gulong)

    
    logger.info('Starting Tiremanila Scraper.')
    ## 3. Tiremanila scraper
    tiremanila_dict = tiremanila_scraper.main(df_gulong)

    
    logger.info('Starting PartsPro Scraper.')
    ## 4. PartsPro scraper
    partspro_dict = partspro_scraper.main(df_gulong)
    
    ## 5. Merge/get intersection of product lists
    df_merged = get_intersection(df_gulong, 
                                 gogulong_dict['df'], 
                                 tiremanila_dict['df'], 
                                 partspro_dict['df'])
    
    ## 6. Save
    if save:
        if platform.lower() in ['all', 'gsheet']:
            # 8. Write to gsheet
            write_to_gsheet(df_merged.fillna('').drop('date', axis=1))
        
        if platform.lower() in ['all', 'bq']:
            bq_dict = init_bq('competitor_price_matching')
            load_save_data(bq_dict, df_merged, 
                           ls = 'save', 
                           mode = 'WRITE_APPEND')
            
    results = {'gulong' : {'source' : 'gulong',
                           'df' : df_gulong,
                           'items' : len(df_gulong),
                           'time_start' : time_start,
                           'time_end' : gulong_time,
                           'duration' :  f'{(gulong_time-time_start).seconds} secs'},
               'gogulong': gogulong_dict,
               'tiremanila' : tiremanila_dict,
               'partspro' : partspro_dict,
               'merged' : {'df' : df_merged,
                           'json' : df_merged.to_json(orient = 'index')}
               }
    
    return results

def main_test():
    
    time_start = dt.now()
    ## 1. Import gulong backend data
    df_gulong = get_gulong_data()
    gulong_time = dt.now()
    
    logger.info('Starting GoGulong Scraper - Test')
    ## 2. Gogulong scraper 
    gogulong_dict = gogulong_scraper.main_test(df_gulong)

    
    logger.info('Starting Tiremanila Scraper - Test')
    ## 3. Tiremanila scraper
    tiremanila_dict = tiremanila_scraper.main_test(df_gulong)

    
    logger.info('Starting PartsPro Scraper - Test')
    ## 4. PartsPro scraper
    partspro_dict = partspro_scraper.main_test(df_gulong)
    
    ## 5. Merge/get intersection of product lists
    df_merged = get_intersection(df_gulong, 
                                 gogulong_dict['df'], 
                                 tiremanila_dict['df'], 
                                 partspro_dict['df'])
    
    results = {'gulong' : {'source' : 'gulong',
                           'df' : df_gulong,
                           'items' : len(df_gulong),
                           'time_start' : time_start,
                           'time_end' : gulong_time,
                           'duration' :  f'{(gulong_time-time_start).seconds} secs'},
               'gogulong': gogulong_dict,
               'tiremanila' : tiremanila_dict,
               'partspro' : partspro_dict,
               'merged' : {'df' : df_merged,
                           'json' : df_merged.to_json(orient = 'index')}
               }
    
    return results

if __name__ == "__main__":
    results = main(save = True, platform = 'all')