import pytest
from airflow.hooks.base import BaseHook

from airflow_tools.data_lake_filesystems.data_lake_factory import DataLakeFactory
from airflow_tools.data_lake_filesystems.impl.sftp_data_lake import SFTPDataLake


@pytest.fixture
def sftp_conn():
    return BaseHook.get_connection('sftp_test')


@pytest.fixture
def sftp_dl(sftp_conn):
    data_lake = DataLakeFactory.get_data_lake_filesystem(sftp_conn)
    assert isinstance(data_lake, SFTPDataLake)
    return data_lake


def test_sftp_write_and_read(sftp_dl):
    TEST_DATA = b'Hello World!'
    DIRECTORY = 'root_folder/foo'
    FILE_PATH = f'{DIRECTORY}/test.txt'

    sftp_dl.hook.create_directory(DIRECTORY)

    sftp_dl.write(TEST_DATA, FILE_PATH)
    assert sftp_dl.read(FILE_PATH) == TEST_DATA

    sftp_dl.delete_prefix(DIRECTORY)
    with pytest.raises(FileNotFoundError):
        sftp_dl.read(FILE_PATH)
