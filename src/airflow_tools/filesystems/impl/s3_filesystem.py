from io import BytesIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow_tools.filesystems.filesystem_protocol import FilesystemProtocol


class S3Filesystem(FilesystemProtocol):
    def __init__(self, hook: S3Hook):
        self.hook = hook

    def read(self, path: str) -> bytes:
        bucket_name, key_name = _get_bucket_and_key_name(path)
        obj = self.hook.get_key(key_name, bucket_name)
        return obj.get()["Body"].read()

    def write(self, data: str | bytes | BytesIO, path: str):
        bucket_name, key_name = _get_bucket_and_key_name(path)
        if isinstance(data, str):
            self.hook.load_string(data, key_name, bucket_name)
        elif isinstance(data, bytes):
            self.hook.load_bytes(data, key_name, bucket_name)
        else:
            self.hook.load_file_obj(data, key_name, bucket_name)

    def delete_file(self, path: str):
        bucket_name, key_name = _get_bucket_and_key_name(path)
        self.hook.delete_objects(bucket_name, [key_name])

    def create_prefix(self, prefix: str):
        bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
        self.hook.create_bucket(bucket_name)
        self.hook.load_string("", key_prefix, bucket_name)

    def delete_prefix(self, prefix: str):
        bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
        self.hook.get_bucket(bucket_name).objects.filter(Prefix=key_prefix).delete()

    def check_file(self, path: str) -> bool:
        bucket_name, key_name = _get_bucket_and_key_name(path)
        return self.hook.check_for_key(key=key_name, bucket_name=bucket_name)

    def check_prefix(self, prefix: str) -> bool:
        bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
        return self.hook.check_for_prefix(
            prefix=key_prefix, bucket_name=bucket_name, delimiter="/"
        )

    def list_files(self, prefix: str) -> list[str]:
        bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
        return [f'{bucket_name}/{obj}' for obj in self.hook.list_keys(bucket_name, key_prefix)]


def _get_bucket_and_key_name(path: str) -> tuple[str, str]:
    parts = path.split("/")
    return parts[0], "/".join(parts[1:])
