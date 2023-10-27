import logging
from io import BytesIO

from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

DataLakeConnection = WasbHook | S3Hook | AwsGenericHook
logger = logging.getLogger(__file__)


class DataLakeFacade:
    """Provides a consistent interface over different Data Lakes (S3/Blob Storage etc.)"""

    def __init__(self, conn: DataLakeConnection):
        self.conn = conn
        if isinstance(self.conn, AwsGenericHook) and not isinstance(self.conn, S3Hook):
            self.conn = S3Hook(self.conn.aws_conn_id)

    def write(self, data: str | bytes | BytesIO, path: str):
        match self.conn.conn_type:
            case "wasb":
                self.conn: WasbHook
                container_name, blob_name = _get_container_and_blob_name(path)
                if isinstance(data, str):
                    self.conn.load_string(data, container_name, blob_name)
                    return

                if isinstance(data, BytesIO):
                    data = data.getvalue()
                logger.info(
                    f'Writing to wasb container "{container_name}" and blob "{blob_name}"'
                )
                self.conn.upload(container_name=container_name, blob_name=blob_name, data=data)
            case "aws":
                self.conn: S3Hook
                bucket_name, key_name = _get_bucket_and_key_name(path)
                if isinstance(data, str):
                    self.conn.load_string(data, key_name, bucket_name)
                elif isinstance(data, bytes):
                    self.conn.load_bytes(data, key_name, bucket_name)
                else:
                    self.conn.load_file_obj(data, key_name, bucket_name)
            case _:
                raise NotImplementedError(
                    f"Data Lake type {self.conn.conn_type} does not support write"
                )


def _get_container_and_blob_name(path: str) -> tuple[str, str]:
    parts = path.split("/")
    return parts[0], "/".join(parts[1:])


def _get_bucket_and_key_name(path: str) -> tuple[str, str]:
    parts = path.split("/")
    return parts[0], "/".join(parts[1:])
