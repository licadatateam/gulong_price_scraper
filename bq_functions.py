# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 12:42:34 2023

@author: Arvin Jay, Carlo
"""

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import json

def get_acct():
    try:
        import streamlit as st
        acct = st.secrets['secrets']
    except:
        with open('secrets.json') as s:
            acct = json.load(s)
    return acct

def authenticate_bq(acct): #acct - service acct details, json type
    credentials = service_account.Credentials.from_service_account_info(
        acct
    )
    client = bigquery.Client(credentials=credentials,project=credentials.project_id)
    return client,credentials

def check_dataset(client,project_id,dataset_name): #check if dataset in BQ is already existing, create dataset if not
    datasets = [i.dataset_id for i in list(client.list_datasets())]
    try:
        if dataset_name not in datasets:
            platform_dataset = "{}.{}".format(project_id,dataset_name) #format "project_id.platform" ie "lica-rdbms.rapide"
            dataset = bigquery.Dataset(platform_dataset)
            dataset.location = "US"
            dataset = client.create_dataset(dataset, timeout=30)
            print("Created dataset {}".format(platform_dataset))
            datasets = [i.dataset_id for i in list(client.list_datasets())]
            print("Updated GCP-Bigquery datasets")
            print(datasets)
        else:
            print("{} already in GCP-Bigquery".format(dataset_name.title()))
        return True
    except:
        print ('Unable to create dataset.')
        return False
    
def check_table(client, table_id):
    status = False
    try:
        client.get_table(table_id)
        print ('Table {} already exists.'.format(table_id))
        status = True
    except NotFound:
        print ('Table {} is not found.'.format(table_id))
    finally:
        return status


def load_config(time_col, auto = True, write_disposition='WRITE_APPEND',src_format=bigquery.SourceFormat.CSV,allow_quoted_newlines =True,partition_type=bigquery.TimePartitioningType.DAY):
    
    if time_col is not None:
        return bigquery.LoadJobConfig(
                    #autodetect = auto,
                    write_disposition = write_disposition, #WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
                    source_format = src_format,
                    allow_quoted_newlines = allow_quoted_newlines,
                    time_partitioning=bigquery.TimePartitioning(
                            type_= partition_type,
                            field= time_col,  # Name of the column to use for partitioning.
                            #expiration_ms=7776000000,  # 90 days.
                        )
                    )
    else:
        return bigquery.LoadJobConfig(
                    #autodetect = auto,
                    write_disposition = write_disposition, #WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
                    source_format = src_format,
                    allow_quoted_newlines = allow_quoted_newlines
                    )

def bq_write(df,credentials,dataset_name,table_name,client, write_mode = 'WRITE_APPEND'):
    try:
        time_col = df.select_dtypes(include = 'datetime').columns[-1]
    except:
        time_col = None
    
    try:
        job_config = load_config(time_col, auto = True, 
                                 write_disposition = write_mode,
                                 src_format = bigquery.SourceFormat.CSV)
    except:
        job_config = load_config(time_col = None, auto = True, 
                                 write_disposition = write_mode,
                                 src_format = bigquery.SourceFormat.CSV)
    
    target_table_id = "{}.{}.{}".format(credentials.project_id,dataset_name,table_name)#project_id.dataset_id.table_id - "lica-rdbms.rapide.MarketBasket"
    
    try:
        job = client.load_table_from_dataframe(df, target_table_id, job_config=job_config)# upload table
    except:
        # no datetime
        job = client.load_table_from_dataframe(df, target_table_id)# upload table
    
    # while job.state != "DONE":
    #     time.sleep(2)
    #     job.reload()

    # print(job.result())
    table = client.get_table(target_table_id)
    report = 'Loaded {} rows and {} columns to {}'.format(
        table.num_rows, len(table.schema), target_table_id)
    
    return report

def write_bq(client, credentials, table_id, data, 
             autodetect : bool = True,
             write_mode : str = 'WRITE_APPEND'):
    # check if table exists
    if check_table(client, table_id):
        pass
    # create table if does not exist
    else:
        table = client.create_table(bigquery.Table(table_id))
        print (f'Table {table_id} created.')
    
    try:
        # set loadjobconfig
        job_config = bigquery.LoadJobConfig()
        # auto_detect schema
        job_config.autodetect = autodetect
        # allow field addition
        job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
        # source format
        job_config.source_format = bigquery.SourceFormat.CSV
        # allow quoted new lines
        job_config.allow_quoted_newlines = True
        # write disposition
        if write_mode in ['WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY']:
            job_config.write_disposition = write_mode.upper()
        else:
            job_config.write_disposition = 'WRITE_APPEND'
    except Exception as e:
        raise e
    
    # load data to table
    job = client.load_table_from_dataframe(data, 
                                           table_id, 
                                           job_config=job_config)# upload table
    
    job.result()
    print("Loaded {} rows into {}.".format(job.output_rows, table_id))
    return job

def query_bq(messages_table_id,client):
    messages_columns = ['thread_id','created_at','from_name','msg_body','to_name'] #columns to query
    messages_query = """
                        SELECT * FROM `{}`
                    """.format(messages_table_id)#standard SQL query
    return client.query(messages_query).to_dataframe() #returns dataframe
