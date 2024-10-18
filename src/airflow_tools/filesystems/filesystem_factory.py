from airflow.hooks.filesystem import FSHook
from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

from airflow_tools.filesystems.filesystem_protocol import FilesystemProtocol
from airflow_tools.filesystems.impl.azure_databricks_volume_filesystem import (
    AzureDatabricksVolumeFilesystem,
)
from airflow_tools.filesystems.impl.azure_file_share_filesystem import (
    AzureFileShareFilesystem,
)
from airflow_tools.filesystems.impl.blob_storage_filesystem import BlobStorageFilesystem
from airflow_tools.filesystems.impl.google_cloud_storage_filesystem import GCSFilesystem
from airflow_tools.filesystems.impl.local_filesystem import LocalFilesystem
from airflow_tools.filesystems.impl.s3_filesystem import S3Filesystem
from airflow_tools.filesystems.impl.sftp_filesystem import SFTPFilesystem
from airflow_tools.providers.azure.hooks.azure_databricks import (
    AzureDatabricksVolumeHook,
)
from airflow_tools.providers.azure.hooks.azure_file_share import (
    AzureFileShareServicePrincipalHook,
)


class FilesystemFactory:
    @staticmethod
    def get_data_lake_filesystem(connection: Connection) -> FilesystemProtocol:
        if connection.conn_type == "wasb":
            hook = WasbHook(wasb_conn_id=connection.conn_id)
            return BlobStorageFilesystem(hook)
        elif connection.conn_type == "aws":
            hook = S3Hook(aws_conn_id=connection.conn_id)
            return S3Filesystem(hook)
        elif connection.conn_type == 'google_cloud_platform':
            hook = GCSHook(gcp_conn_id=connection.conn_id)
            return GCSFilesystem(hook)
        elif connection.conn_type == "sftp":
            hook = SFTPHook(ssh_conn_id=connection.conn_id)
            return SFTPFilesystem(hook)
        elif connection.conn_type == "fs":
            hook = FSHook(fs_conn_id=connection.conn_id)
            return LocalFilesystem(hook)
        elif connection.conn_type == "azure_file_share_sp":
            hook = AzureFileShareServicePrincipalHook(conn_id=connection.conn_id)
            return AzureFileShareFilesystem(hook)
        elif connection.conn_type == "azure_databricks_volume":
            hook = AzureDatabricksVolumeHook(
                azure_databricks_volume_conn_id=connection.conn_id
            )
            return AzureDatabricksVolumeFilesystem(hook)
        else:
            raise NotImplementedError(
                f"Data Lake type {connection.conn_type} is not supported"
            )
