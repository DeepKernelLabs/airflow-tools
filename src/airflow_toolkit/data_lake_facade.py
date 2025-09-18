import logging
from io import BytesIO
from typing import Union

from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

DataLakeConnection = Union[WasbHook, S3Hook, AwsGenericHook]
logger = logging.getLogger(__file__)


class DataLakeFacade:
    """Provides a consistent interface over different Data Lakes (S3/Blob Storage etc.)"""

    def __init__(self, conn: Union[WasbHook, S3Hook, AwsGenericHook]):
        logger.warning(
            "DataLakeFacade is deprecated and will be removed in a future version. "
            "Please use FilesystemFactory instead."
        )
        self.conn: S3Hook | WasbHook = (
            S3Hook(conn.aws_conn_id)
            if isinstance(conn, AwsGenericHook) and not isinstance(conn, S3Hook)
            else conn
        )

    def write(self, data: str | bytes | BytesIO, path: str):
        match self.conn.conn_type:
            case "wasb":
                assert isinstance(self.conn, WasbHook)
                container_name, blob_name = _get_container_and_blob_name(path)
                if isinstance(data, str):
                    self.conn.load_string(data, container_name, blob_name)
                    return

                if isinstance(data, BytesIO):
                    data = data.getvalue()
                logger.info(
                    f'Writing to wasb container "{container_name}" and blob "{blob_name}"'
                )
                self.conn.upload(
                    container_name=container_name, blob_name=blob_name, data=data
                )
            case "aws":
                assert isinstance(self.conn, S3Hook)
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

    def delete_prefix(self, prefix: str):
        match self.conn.conn_type:
            case "wasb":
                assert isinstance(self.conn, WasbHook)
                container_name, blob_prefix = _get_container_and_blob_name(prefix)
                self.conn.delete_file(
                    container_name, blob_prefix, is_prefix=True, ignore_if_missing=True
                )
            case "aws":
                assert isinstance(self.conn, S3Hook)
                bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
                self.conn.get_bucket(bucket_name).objects.filter(
                    Prefix=key_prefix
                ).delete()
            case _:
                raise NotImplementedError(
                    f"Data Lake type {self.conn.conn_type} does not support delete_prefix"
                )

    def check_prefix(self, prefix: str) -> bool:
        match self.conn.conn_type:
            case "wasb":
                assert isinstance(self.conn, WasbHook)
                container_name, blob_prefix = _get_container_and_blob_name(prefix)
                return bool(
                    self.conn.get_blobs_list(
                        container_name=container_name, prefix=blob_prefix
                    )
                )
            case "aws":
                assert isinstance(self.conn, S3Hook)
                bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
                object_list_at_bucket = list(
                    self.conn.get_bucket(bucket_name)
                    .objects.filter(Prefix=key_prefix)
                    .all()
                )
                return bool(object_list_at_bucket)
            case _:
                raise NotImplementedError(
                    f"Data Lake type {self.conn.conn_type} does not support check_for_prefix"
                )


def _get_container_and_blob_name(path: str) -> tuple[str, str]:
    parts = path.split("/")
    return parts[0], "/".join(parts[1:])


def _get_bucket_and_key_name(path: str) -> tuple[str, str]:
    parts = path.split("/")
    return parts[0], "/".join(parts[1:])
