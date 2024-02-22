import pytest
from airflow.hooks.base import BaseHook

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
