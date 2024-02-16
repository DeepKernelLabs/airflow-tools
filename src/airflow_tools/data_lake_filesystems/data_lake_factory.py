from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

from airflow_tools.data_lake_filesystems.data_lake_protocol import DataLakeProtocol
from airflow_tools.data_lake_filesystems.impl.blob_storage_data_lake import (
    BlobStorageDataLake,
)
from airflow_tools.data_lake_filesystems.impl.s3_data_lake import S3DataLake
from airflow_tools.data_lake_filesystems.impl.sftp_data_lake import SFTPDataLake


class DataLakeFactory:
    @staticmethod
    def get_data_lake_filesystem(connection: Connection) -> DataLakeProtocol:
        if connection.conn_type == "wasb":
            hook = WasbHook(wasb_conn_id=connection.conn_id)
            return BlobStorageDataLake(hook)
        elif connection.conn_type == "aws":
            hook = S3Hook(aws_conn_id=connection.conn_id)
            return S3DataLake(hook)
        elif connection.conn_type == "sftp":
            hook = SFTPHook(ssh_conn_id=connection.conn_id)
            return SFTPDataLake(hook)
        else:
            raise NotImplementedError(
                f"Data Lake type {connection.conn_type} is not supported"
            )
