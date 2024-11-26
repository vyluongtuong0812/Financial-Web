# Pull stores data bhx
import pandas as pd
import sqlalchemy
import requests
from prefect import task, flow
from prefect.logging import get_run_logger
from prefect.blocks.system import Secret
from sqlalchemy import text, exc
from src.common.utils import prepare_driver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException


TABLE = 'Retail_Stores'
DATABASE = 'Retail'



@task(task_run_name=f'clean_{TABLE}', retries=3, retry_delay_seconds=30)
def clean_old_data(dt):
    '''Clean old data from database'''
    logger = get_run_logger()
    secret_block = Secret.load("mssql-conn")
    uri = secret_block.get()
    engine = sqlalchemy.create_engine(uri.format(db=DATABASE), use_setinputsizes=False)
    with engine.connect() as conn:
        try: 
            sql = f'''
                DELETE FROM {TABLE}
                WHERE CreatedDT = '{dt}' and store_name = 'bhx'
                '''
            logger.debug(sql)
            conn.execute(text(sql))
            conn.commit()
        except exc.SQLAlchemyError as e:
            conn.rollback()
            raise e


@task(task_run_name='get_bhx_stores',retries=3, retry_delay_seconds=30)
def pull_stores(dt_: str):
    secret_block = Secret.load("mssql-conn")
    uri = secret_block.get()
    engine = sqlalchemy.create_engine(uri.format(db=DATABASE), use_setinputsizes=False)
    
    query = 'SELECT provinceid, name FROM bhx_provinceid'
    province_data = pd.read_sql(query, engine)      
    all_store_info = []
    
    for index, row in province_data.iterrows():
        param1 = row['provinceid']

        params = {
            'provinceId': param1
        }

# đổi thành api của BHX
        url = 'http://api/other/store' 
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  
            response_data = response.json()
            if response_data.get('code') == 0: 
                stores = response_data['data']['stores']  
                for store in stores:
                    display_address = store['displayStoreAddress'] if store['displayStoreAddress'] else store['storeName']
                    store_info = {
                        "displayStoreAddress": display_address,
                        "lat": store['lat'],
                        "lng": store['lng'],
                        "districtId": store['districtId'],
                        "provinceId": store['provinceId']
                    }
                    all_store_info.append(store_info)
            else:
                logger = get_run_logger()
                logger.error(f"Unexpected response code in API response: {response_data}")
        except requests.exceptions.RequestException as e:
            logger = get_run_logger()
            logger.error(f"Request error: {e}")

    df = pd.DataFrame(all_store_info)
    df = df.merge(province_data[['provinceid', 'name']], left_on='provinceId', right_on='provinceid', how='left')
    
    df.rename(columns={
        'displayStoreAddress': 'address',
        'lng': 'long',
        'districtId': 'district',
        'name': 'city'
    }, inplace=True)
    df['status'] = 1
    df['store_name'] = 'bhx'
    df['CreatedDT'] = pd.to_datetime(dt_)
    df['date'] = df['CreatedDT']
    df.drop_duplicates(inplace=True)
    columns_to_keep = ['date', 'address', 'city', 'district', 'lat', 'long', 'status', 'store_name', 'CreatedDT']
    df = df[columns_to_keep]
    return df

            
@task(name='insert_bhx_stores', retries=3, retry_delay_seconds=30)
def insert_data(data: pd.DataFrame):
    logger = get_run_logger()
    if data is None:
        logger.info('No data to insert')
        return None
    logger = get_run_logger()
    logger.info(data.head(5))
    logger.info('Saving data')
    secret_block = Secret.load("mssql-conn")
    uri = secret_block.get()
    engine = sqlalchemy.create_engine(uri.format(db=DATABASE), use_setinputsizes=False)
    data.to_sql(TABLE, engine, if_exists='append', index=False)

@flow(name='branch_Retail_bhxStores')
def Retail_bhx(task_kwargs:dict, flow_kwargs:dict):
    data = pull_stores.submit(flow_kwargs['dt'])
    clean = clean_old_data.submit(flow_kwargs['dt'], wait_for=[data])
    insert_data.with_options(tags=['slow-db']).submit(data, wait_for=[clean])

