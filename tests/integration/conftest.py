import pytest
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database as _create_database

from airflow_tools.filesystems.filesystem_factory import FilesystemFactory
from airflow_tools.filesystems.impl.sftp_filesystem import SFTPFilesystem


@pytest.fixture
def sftp_conn():
    return BaseHook.get_connection('sftp_test')


@pytest.fixture
def sftp_dl(sftp_conn):
    data_lake = FilesystemFactory.get_data_lake_filesystem(sftp_conn)
    assert isinstance(data_lake, SFTPFilesystem)
    return data_lake


@pytest.fixture
def sqlite_database(tmp_path) -> str:
    db_file_path = tmp_path / 'test.db'
    url = f'sqlite:///{db_file_path}'
    engine = create_engine(url)
    _create_database(engine.url)

    engine.execute('DROP TABLE IF EXISTS TESTING')
    engine.execute(
        'CREATE TABLE TESTING (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)'
    )
    engine.execute('INSERT INTO TESTING (name, age) VALUES ("John", 25)')
    engine.execute('INSERT INTO TESTING (name, age) VALUES ("Jane", 30)')
    engine.execute('INSERT INTO TESTING (name, age) VALUES ("Mary", 35)')
    engine.execute('INSERT INTO TESTING (name, age) VALUES ("Tommy", 40)')
    engine.execute('INSERT INTO TESTING (name, age) VALUES ("Jerry", 45)')

    return db_file_path
