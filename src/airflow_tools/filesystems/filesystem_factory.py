from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

from airflow_tools.filesystems.filesystem_protocol import FilesystemProtocol
from airflow_tools.filesystems.impl.blob_storage_filesystem import BlobStorageFilesystem
from airflow_tools.filesystems.impl.s3_filesystem import S3Filesystem
from airflow_tools.filesystems.impl.sftp_filesystem import SFTPFilesystem


class FilesystemFactory:
    @staticmethod
    def get_data_lake_filesystem(connection: Connection) -> FilesystemProtocol:
        if connection.conn_type == "wasb":
            hook = WasbHook(wasb_conn_id=connection.conn_id)
            return BlobStorageFilesystem(hook)
        elif connection.conn_type == "aws":
            hook = S3Hook(aws_conn_id=connection.conn_id)
            return S3Filesystem(hook)
        elif connection.conn_type == "sftp":
            hook = SFTPHook(ssh_conn_id=connection.conn_id)
            return SFTPFilesystem(hook)
        else:
            raise NotImplementedError(
                f"Data Lake type {connection.conn_type} is not supported"
            )
