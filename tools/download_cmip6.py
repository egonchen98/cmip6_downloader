import datetime
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
import json

import requests
import pandas as pd
from pathlib import Path

import wget
from selenium.webdriver.firefox.service import Service
from selenium import webdriver


def get_wget_files() -> None:
    """Get(download) wget.sh from a html file using selenium
    1. get every item
    2. click on 'get wget'
    3. wait a second to click another one and loop."""
    driver = webdriver.Firefox(service=Service(executable_path=r'D:\OneDrive - HHU\MainSync\03_Life\02_Self\scripts\geckodriver.exe'))
    driver.get('file:///E:/CMIP6 Training/code/resources/download_links/cart.html')
    try:
        driver.implicitly_wait(1)
        elements = driver.find_elements(by='partial link text', value='WGET Script')
        index = 0
        for elem in elements[1:]:
            index = index + 1
            elem.click()
            driver.implicitly_wait(1)
            if index % 10 == 0:
                print(index)
    except Exception as e:
        print(e)
    finally:
        driver.close()


def get_url() -> pd.DataFrame:
    """Get download url from wget.sh files"""
    wget_file_folder = '../resources/download_links'
    dfs = []
    for file in os.listdir(wget_file_folder):
        if not file.endswith('.sh'):
            continue
        path = Path(f'{wget_file_folder}/{file}')
        text = path.read_text()
        target_text = re.findall("'http://.*nc'", text, flags=re.MULTILINE)
        df = pd.DataFrame(target_text, columns=['url'])
        df['place'] = file
        df['filename'] = df['url'].str.split('/').apply(lambda row: row[-1])
        dfs.append(df)

    df = pd.concat(dfs)
    df['url'] = df['url'].str.strip("'")
    df['filename'] = df['filename'].str.strip("'")
    df[['param', 'frequency', 'model', 'scenario', 'r1i1p1f1', 'gn', 'date_range']] = \
        df['filename'].str.split('_', expand=True)
    df[['st_date', 'end_date']] = df['date_range'].str.split('-', expand=True)
    df[['end_date', 'file_type']] = df['end_date'].str.split('.', expand=True)
    df.drop(columns=['frequency', 'r1i1p1f1', 'date_range', 'file_type'], inplace=True)
    df.dropna(how='any', axis=0, inplace=True)
    df[['st_date', 'end_date']] = df[['st_date', 'end_date']].astype('int32')
    df = df.loc[~((df.end_date < 19510101) | (df.st_date > 21010101))]

    return df


def download_with_wget(url: str, path: str) -> None:
    """Download file with wget library"""
    wget.download(url, path)
    print(f'--- {path} downloaded.')


def download_files(df: pd.DataFrame, target_folder: str, count_limit=4) -> None:
    """Download files from url with idm"""
    df = df.loc[df.end_date - df.st_date < 2e4]
    params = df['param'].unique()
    pool = ThreadPoolExecutor(max_workers=20)
    count = 0
    for param in params:
        # Check param folder existence
        if not os.path.exists(f'{target_folder}/{param}'):
            os.makedirs(f'{target_folder}/{param}')
        df_param = df.loc[df.param == param]
        models = df_param['model'].unique()
        for model in models:
            df_model = df_param.loc[df_param.model == model]
            scenarios = df_model['scenario'].unique()
            if len(scenarios) < 2:  # If only one scenario=historical, skip
                continue
            # Check if model directory exists
            if not os.path.exists(f'{target_folder}/{param}/{model}'):
                os.makedirs(f'{target_folder}/{param}/{model}')
            for scenario in scenarios:
                # Check if scenario directory exists
                if not os.path.exists(f'{target_folder}/{param}/{model}/{scenario}'):
                    os.makedirs(f'{target_folder}/{param}/{model}/{scenario}')

                # Download files with url
                for index, row in df_model.iterrows():
                    if not os.path.exists(f'{target_folder}/{param}/{model}/{scenario}/{row.filename}'):
                        # Check url http connection with requests library
                        url = row.url
                        status_code = requests.head(url).status_code
                        if status_code != 200:
                            continue

                        pool.submit(download_with_wget, row.url, f'{target_folder}/{param}/{model}/{scenario}/{row.filename}')
                        # download_with_wget(url=row.url,
                        #                    path=f'{target_folder}/{param}/{model}/{scenario}/{row.filename}',)
                        time.sleep(0.1)
                        count = count + 1
                        if count % 50 == 0:
                            print(f'Downloaded {count} files')

    pool.shutdown()


def run(df: pd.DataFrame):
    """run"""
    # get_wget_files()
    # df = get_url()
    # print(df.to_string())
    config = json.loads(Path('../resources/config.json').read_text())
    database = Path(config['Database_folder'])
    if not os.path.exists(database):
        os.makedirs(database)
    download_files(df=df, target_folder=database)


if __name__ == '__main__':
    pass
