from airflow.hooks.base import BaseHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from airflow_tools.filesystems.filesystem_factory import FilesystemFactory


class FilesystemFileSensor(BaseSensorOperator):
    """
    Check if a file exists in the specified storage.

    This sensor use connection type (`connection.conn_type`) to determine the
    filesystem type.

    Current supported filesystems:

    - 'aws' (S3)
    - 'azure_databricks_volume'
    - 'azure_file_share_sp'
    - 'sftp'
    - 'fs' (LocalFileSystem)
    - 'google_cloud_platform'
    - 'wasb' (BlobStorageFilesystem)

    :param filesystem_conn_id: connection id for the filesystem.
    :param source_path: The path to the file (include bucket name).

    """

    template_fields = ('source_path',)

    def __init__(self, filesystem_conn_id: str, source_path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_path = source_path
        self.filesystem_conn_id = filesystem_conn_id

    def poke(self, context: Context):
        """
        Check if the file exists in the specified storage.
        """
        conn = BaseHook.get_connection(self.filesystem_conn_id)
        fs = FilesystemFactory.get_data_lake_filesystem(conn)

        return fs.check_file(self.source_path)
