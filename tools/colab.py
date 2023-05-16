import pandas as pd
import pymysql
from sqlalchemy import create_engine
import cProfile
import os
import json
from pathlib import Path


def write_df(df: pd.DataFrame) -> None:
    """Write file table to mysql"""
    conn = create_engine('mysql+pymysql://colab:colab123456@124.220.27.50/colab')

    df.to_sql('cmip6_file_manager', conn, if_exists='replace', index=False)

    return None


def get_url_to_download() -> pd.DataFrame:
    """Get dataframe from mysql and write logged dataframe to mysql"""
    conn = create_engine('mysql+pymysql://colab:colab123456@124.220.27.50/colab')
    df = pd.read_sql('cmip6_file_manager', conn)
    try:
        df1 = df.loc[df.status == 'not requested'].sample(n=50)
    except Exception as e:
        print('All file downloaded......,\n You"ve finished your job, Thanks!!\n\n------------')
        job_finished = True
        return None
    df1['status'] = 'downloading'
    df.update(df1)
    df.to_sql('cmip6_file_manager', conn, if_exists='replace', index=False)
    return df


def log_downloaded_files() -> None:
    """Log downloaded files"""
    cur_dir = os.path.dirname(__file__)
    root_dir = Path(os.path.dirname(cur_dir))
    config = json.loads(open(f'{root_dir}/resources/config.json').read())
    database = Path(config['Database_folder'])

    filelist = []
    # Get all files under the database folder
    for root, dirs, files in os.walk(database):
        for file in files:
            if file.endswith('.nc'):
                filelist.append(file)

    df1 = pd.DataFrame(filelist, columns=['filename'])
    # get mysql dataframe
    df = pd.read_sql('cmip6_file_manager', create_engine('mysql+pymysql://colab:colab123456@124.220.27.50/colab'))
    df2 = pd.merge(df, df1, on='filename', how='inner')
    print(len(df2))
    df2['status'] = 'downloaded'
    df.update(df2)

    df.to_sql('cmip6_file_manager', create_engine('mysql+pymysql://colab:colab123456@124.220.27.50/colab'), if_exists='replace', index=False)
    return None


def run() -> None:
    """Run"""
    # reset database in mysql
    df = pd.read_csv('../resources/res.csv')
    df['status'] = 'not requested'
    write_df(df=df)


def test():
    project_name = 'code'
    cur_path = os.path.dirname(__file__)
    path2 = os.path.dirname(cur_path)
    path = os.path.join(path2, 'config.json')
    print(cur_path, path2, path)


if __name__ == '__main__':
    run()
