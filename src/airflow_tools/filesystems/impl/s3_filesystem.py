from io import BytesIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow_tools.filesystems.filesystem_protocol import FilesystemProtocol


class S3Filesystem(FilesystemProtocol):
    def __init__(self, hook: S3Hook):
        self.hook = hook

    def read(self, path: str) -> bytes:
        bucket_name, key_name = _get_bucket_and_key_name(path)
        obj = self.hook.get_key(key_name, bucket_name)
        return obj.get()["Body"]

    def write(self, data: str | bytes | BytesIO, path: str):
        bucket_name, key_name = _get_bucket_and_key_name(path)
        if isinstance(data, str):
            self.conn.load_string(data, key_name, bucket_name)
        elif isinstance(data, bytes):
            self.conn.load_bytes(data, key_name, bucket_name)
        else:
            self.conn.load_file_obj(data, key_name, bucket_name)

    def delete_prefix(self, prefix: str):
        bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
        self.conn.get_bucket(bucket_name).objects.filter(Prefix=key_prefix).delete()


def _get_bucket_and_key_name(path: str) -> tuple[str, str]:
    parts = path.split("/")
    return parts[0], "/".join(parts[1:])
