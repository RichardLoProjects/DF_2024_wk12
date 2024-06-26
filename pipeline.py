import requests
import pandas as pd
import time
import numpy as np
import os
from dotenv import load_dotenv # type: ignore
import psycopg2 as psql # type: ignore
import warnings

class EnvSecrets:
    def __init__(self) -> None:
        load_dotenv()
        self.api1_static = os.getenv('PRIMARY_API_STATIC_URL')
        self.api1_dynamic = os.getenv('PRIMARY_API_DYNAMIC_URL')
        self.api1_user_agent = os.getenv('PRIMARY_API_USER_AGENT')
        self.api1_user_id = os.getenv('PRIMARY_API_USER_ID')
        self.api2_backup = os.getenv('SECONDARY_API_URL')
        self.db_name = os.getenv('DATABASE_NAME')
        self.db_host = os.getenv('DATABASE_HOST')
        self.db_port = int(os.getenv('DATABASE_PORT'))
        self.sql_user = os.getenv('SQL_USERNAME')
        self.sql_pass = os.getenv('SQL_PASSWORD')
        self.tname_static = os.getenv('SQL_TABLENAME_FOR_STATIC_DATA')
        self.tname_dynamic = os.getenv('SQL_TABLENAME_FOR_DYNAMIC_DATA')

class DatabaseConnection:
    def __init__(self, secret:EnvSecrets) -> None:
        self.connection = psql.connect(
            database = secret.db_name
            , host = secret.db_host
            , port = secret.db_port
            , user = secret.sql_user
            , password = secret.sql_pass
        )
        self.cursor = self.connection.cursor()
    def close(self) -> None:
        self.connection.close()

class DataPipeline:
    def __init__(self) -> None:
        self._df = pd.DataFrame()
    def extract(self, secret:EnvSecrets, polymorphism) -> None:
        self._df = polymorphism(secret)
    def transform(self) -> None:
        for column in self._df.columns:
            if self._df[column].dtype == 'object':
                try:
                    self._df[column] = self._df[column].astype(float)
                except ValueError:
                    pass
            if self._df[column].dtype == 'float64':
                try:
                    self._df[column] = self._df[column].astype(int)
                except ValueError:
                    pass
            if 'timestamp' in column:
                try:
                    self._df[column] = pd.to_datetime(self._df[column], unit='s')
                except:
                    pass
    def load(self, secret, database, polymorphism) -> None:
        polymorphism(secret, database)

def pipeline1_static_extract(secret:EnvSecrets) -> pd.DataFrame:
    _header = {
        'User-Agent': secret.api1_user_agent,
        'From': secret.api1_user_id
    }
    _json = requests.get(secret.api1_static, headers=_header).json()
    _df:pd.DataFrame = pd.concat([pd.DataFrame.from_dict([item], orient='columns') for item in _json])
    _df.set_index('id', inplace=True)
    _df.sort_values('id', ascending=True, inplace=True)
    _df.reset_index(inplace=True)
    _df = _df[['id', 'name', 'members', 'limit', 'value', 'highalch', 'lowalch', 'examine', 'icon']]
    _df.rename(columns={
        'id':'item_id'
        , 'name':'item_name'
        , 'limit':'buy_limit'
        , 'highalch':'high_alch'
        , 'lowalch':'low_alch'
    }, inplace=True)
    return _df

def pipeline1_dynamic_extract(secret:EnvSecrets) -> pd.DataFrame:
    _header = {
        'User-Agent': secret.api1_user_agent,
        'From': secret.api1_user_id
    }
    _json = requests.get(secret.api1_dynamic, headers=_header).json()
    _df:pd.DataFrame = pd.DataFrame()
    for item_id, item in _json['data'].items():
        _df_row = {
            'item_id': item_id
            , 'price_timestamp': _json.get('timestamp', int(time.time()))
            , 'avg_high_price': item.get('avgHighPrice')
            , 'avg_low_price': item.get('avgLowPrice')
            , 'high_price_volume': item.get('highPriceVolume')
            , 'low_price_volume': item.get('lowPriceVolume')
        }
        for k,v in _df_row.items():
            try:
                _df_row[k] = int(v)
            except:
                pass
        temp_df = pd.DataFrame.from_dict([_df_row], orient='columns')
        _df = pd.concat([_df, temp_df])
    _df.set_index('item_id', inplace=True)
    _df.sort_values('item_id', ascending=True, inplace=True)
    _df.reset_index(inplace=True)
    return _df

def pipeline1_static_load(secret, database):
    pass

def pipeline1_dynamic_load(secret, database):
    pass


def main() -> None:
    warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy connectable.*")
    my_secrets = EnvSecrets()
    my_database = DatabaseConnection(my_secrets)
    if 1==0: # if static DB does not exist then run data pipeline for static data
        pipeline1_static = DataPipeline()
        pipeline1_static.extract(my_secrets, pipeline1_static_extract)
        pipeline1_static.transform()
        pipeline1_static.load(my_secrets, my_database, )
    pipeline1_dynamic = DataPipeline()
    pipeline1_dynamic.extract(my_secrets, pipeline1_dynamic_extract)
    pipeline1_dynamic.transform()
    pipeline1_dynamic.load(my_secrets, my_database, )
    my_database.close()

if __name__ == '__main__':
    main()


#todo list: create tables and add data within load polymorphism