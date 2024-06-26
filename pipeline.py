import requests
import pandas as pd
import os
from dotenv import load_dotenv
import psycopg2 as psql
import warnings


class EnvSecrets:
    def __init__(self) -> None:
        load_dotenv()
        self._API_URL:str = os.getenv('API_URL')
        self._SQLHOST:str = os.getenv('SQL_HOST')
        self._SQLUSER:str = os.getenv('SQL_USERNAME')
        self._SQLPASS:str = os.getenv('SQL_PASSWORD')
        self._SQLTABLE_STATIC:str = os.getenv('SQL_TABLENAME_STATIC')
        self._SQLTABLE_DYNAMIC:str = os.getenv('SQL_TABLENAME_DYNAMIC')
    @property
    def api(self) -> str:
        return self._API_URL
    @property
    def sql_host(self) -> str:
        return self._SQLHOST
    @property
    def sql_user(self) -> str:
        return self._SQLUSER
    @property
    def sql_pass(self) -> str:
        return self._SQLPASS
    @property
    def table_static(self) -> str:
        return self._SQLTABLE_STATIC
    @property
    def table_dynamic(self) -> str:
        return self._SQLTABLE_DYNAMIC


class AbstractDataPipeline:
    def __init__(self) -> None:
        if self.__class__ is AbstractDataPipeline:
            raise TypeError('Cannot instantiate abstract class.')
    def extract(self) -> None:
        raise NotImplementedError('Cannot call method from abstract class.')
    def transform(self) -> None:
        raise NotImplementedError('Cannot call method from abstract class.')
    def load(self) -> None:
        raise NotImplementedError('Cannot call method from abstract class.')


def main() -> None:
    warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy connectable.*")
    my = EnvSecrets()

if __name__ == '__main__':
    main()