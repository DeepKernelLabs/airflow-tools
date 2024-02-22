import pytest
from airflow.hooks.base import BaseHook

from airflow_tools.filesystems.filesystem_factory import FilesystemFactory
from airflow_tools.filesystems.impl.sftp_filesystem import SFTPFilesystem


@pytest.fixture
def sftp_conn():
    return BaseHook.get_connection('sftp_test')


@pytest.fixture
def sftp_fs(sftp_conn):
    fs = FilesystemFactory.get_data_lake_filesystem(sftp_conn)
    assert isinstance(fs, SFTPFilesystem)
    return fs


def test_sftp_write_and_read(sftp_fs):
    TEST_DATA = b'Hello World!'
    DIRECTORY = 'root_folder/foo'
    FILE_PATH = f'{DIRECTORY}/test.txt'

    sftp_fs.hook.create_directory(DIRECTORY)

    sftp_fs.write(TEST_DATA, FILE_PATH)
    assert sftp_fs.read(FILE_PATH) == TEST_DATA

    sftp_fs.delete_prefix(DIRECTORY)
    with pytest.raises(FileNotFoundError):
        sftp_fs.read(FILE_PATH)
