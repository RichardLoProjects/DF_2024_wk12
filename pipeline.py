'''Libraries'''
import requests
import pandas as pd
import time
import numpy as np
import os
from dotenv import load_dotenv # type: ignore
import psycopg2 as psql # type: ignore
import warnings



'''Classes'''
class EnvSecrets:
    def __init__(self) -> None:
        '''Fetch all sensitive data and load into variables for later use.'''
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
        '''Open database connection.'''
        try:
            self.connection = psql.connect(
                database = secret.db_name
                , host = secret.db_host
                , port = secret.db_port
                , user = secret.sql_user
                , password = secret.sql_pass
            )
            self.cursor = self.connection.cursor()
            self.valid = True
        except:
            '''Possible expansion for email sending, notifying database failure or invalid credentials.'''
            self.valid = False
    def close(self) -> None:
        '''Close database connection.'''
        try:
            self.connection.close()
        except:
            pass

class DataPipeline:
    def __init__(self, secret) -> None:
        self._df = pd.DataFrame()
        self._secret:EnvSecrets = secret
    def extract(self, polymorphism) -> None:
        '''Get request from API. Create dataframe. Populate dataframe'''
        self._df = polymorphism(self._secret)
    def transform(self, polymorphism) -> None:
        '''Data cleaning and feature engineering.'''
        self._dtype_cleaning()
        self._df = polymorphism(self._df)
    def load(self, database, polymorphism) -> None:
        '''Create table if not exist. Filter and commit delta of new data.'''
        polymorphism(self._secret, self._df, database)
        database.connection.commit()
    def _dtype_cleaning(self) -> None:
        '''Correcting data types'''
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



'''Polymorphism'''
def pipeline1_static_extract(secret:EnvSecrets) -> pd.DataFrame:
    _header = {
        'User-Agent': secret.api1_user_agent,
        'From': secret.api1_user_id
    }
    _json = requests.get(secret.api1_static, headers=_header).json() # r.get().status_code
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
        if _df_row.get('avg_high_price') and _df_row.get('avg_low_price'):
            _high = _df_row['avg_high_price']
            _low = _df_row['avg_low_price']
            _df_row['avg_high_price'], _df_row['avg_low_price'] = max(_high, _low), min(_high, _low)
        temp_df = pd.DataFrame.from_dict([_df_row], orient='columns')
        _df = pd.concat([_df, temp_df])
    _df.set_index('item_id', inplace=True)
    _df.sort_values('item_id', ascending=True, inplace=True)
    _df.reset_index(inplace=True)
    return _df

def pipeline1_static_transform(_df) -> pd.DataFrame:
    return _df

def pipeline1_dynamic_transform(_df) -> pd.DataFrame:
    pa = _df['avg_high_price']
    pb = _df['avg_low_price']
    va = _df['high_price_volume']
    vb = _df['low_price_volume']
    _df['total_volume'] = va + vb
    _df['avg_mid_price'] = (pa + pb)//2
    _df['avg_micro_price'] = np.where(
        va + vb != 0
        , ((pa*vb)//(va+vb)) + ((pb*va)//(va+vb))
        , None
    )
    _df['wide_spread'] = np.where(
        (pa.notna() & pb.notna()) & (pa-np.minimum(5_000_000,pa//100) > pb+1_000_000)
        , True
        , False
    )
    return _df

def pipeline1_static_load(secret:EnvSecrets, _df, database):
    primary_key = 'item_id'
    column_definitions = []
    for column_name, dtype in zip(_df.columns, _df.dtypes):
        if pd.api.types.is_integer_dtype(dtype):
            sql_type = 'int'
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = 'float'
        elif pd.api.types.is_bool_dtype(dtype):
            sql_type = 'boolean'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            sql_type = 'timestamp'
        else:
            sql_type = 'varchar(1000)'
        if column_name == primary_key:
            column_definitions.append(f'{column_name} {sql_type} PRIMARY KEY')
        else:
            column_definitions.append(f'{column_name} {sql_type}')
    create_table_sql = f'CREATE TABLE IF NOT EXISTS {secret.tname_static} ({", ".join(column_definitions)});'
    database.cursor.execute(create_table_sql)
    _df = _df.replace({np.nan: None})
    query = f'SELECT {primary_key} FROM {secret.tname_static}'
    existing_data = pd.read_sql(query, database.connection)
    existing_set = set(existing_data[primary_key])
    non_duplicate_data = _df[~_df[primary_key].isin(existing_set)]
    if not non_duplicate_data.empty:
        tuples = [tuple(x) for x in non_duplicate_data.to_numpy()]
        columns = ','.join(non_duplicate_data.columns)
        values = ','.join(['%s'] * len(non_duplicate_data.columns))
        insert_query = f'INSERT INTO {secret.tname_static} ({columns}) VALUES ({values})'
        database.cursor.executemany(insert_query, tuples)

def pipeline1_dynamic_load(secret:EnvSecrets, _df, database):
    create_table_sql = f'''
CREATE TABLE IF NOT EXISTS {secret.tname_dynamic} (
    item_id int
    , price_timestamp timestamp
    , avg_high_price int
    , avg_low_price int
    , high_price_volume int
    , low_price_volume int
    , total_volume int
    , avg_mid_price float
    , avg_micro_price float
    , wide_spread boolean
    , PRIMARY KEY (item_id, price_timestamp)
    , FOREIGN KEY (item_id) REFERENCES {secret.tname_static}(item_id)
);
'''
    database.cursor.execute(create_table_sql)
    _df = _df.replace({np.nan: None})
    composite_key_columns = ['item_id', 'price_timestamp']
    query = f'SELECT {", ".join(composite_key_columns)} FROM {secret.tname_dynamic}'
    existing_data = pd.read_sql(query, database.connection)
    existing_set = set([tuple(x) for x in existing_data.to_numpy()])
    incoming_data_set = set([tuple(x) for x in _df[composite_key_columns].to_numpy()])
    non_duplicate_keys = incoming_data_set - existing_set
    non_duplicate_data = _df[
        _df.apply(lambda row: tuple(row[composite_key_columns]), axis=1).isin(non_duplicate_keys)
    ]
    if not non_duplicate_data.empty:
        tuples = [tuple(x) for x in non_duplicate_data.to_numpy()]
        columns = ','.join(non_duplicate_data.columns)
        values = ','.join(['%s'] * len(non_duplicate_data.columns))
        insert_query = f'INSERT INTO {secret.tname_dynamic} ({columns}) VALUES ({values})'
        database.cursor.executemany(insert_query, tuples)

def send_email(error_message):
    pass



'''Main'''
def main() -> None:
    warnings.filterwarnings('ignore', message='.*pandas only supports SQLAlchemy connectable.*')
    my_secrets = EnvSecrets()
    my_database = DatabaseConnection(my_secrets)
    try:
        sql = f'SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = \'{my_secrets.api1_static}\');'
        my_database.cursor.execute(sql)
        table_exist = my_database.cursor.fetchall()[0][0]
        if  not table_exist:
            pipeline1_static = DataPipeline(my_secrets)
            pipeline1_static.extract(pipeline1_static_extract)
            pipeline1_static.transform(pipeline1_static_transform)
            pipeline1_static.load(my_database, pipeline1_static_load)
        pipeline1_dynamic = DataPipeline(my_secrets)
        pipeline1_dynamic.extract(pipeline1_dynamic_extract)
        pipeline1_dynamic.transform(pipeline1_dynamic_transform)
        pipeline1_dynamic.load(my_database, pipeline1_dynamic_load)
    except Exception as e:
        send_email(e)
        '''Emergency csv dump'''
        try:
            pipeline1_dynamic = DataPipeline(my_secrets)
            pipeline1_dynamic.extract(pipeline1_dynamic_extract)
            pipeline1_dynamic.transform(pipeline1_dynamic_transform)
            pipeline1_dynamic._df.to_csv(f'osrs_emergency_csv_dump/{int(time.time())}.csv')
        except:
            pass
    finally:
        my_database.close()

if __name__ == '__main__':
    main()
