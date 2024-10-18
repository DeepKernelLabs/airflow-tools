from io import BytesIO

from airflow.providers.google.cloud.hooks.gcs import GCSHook

from airflow_tools.filesystems.filesystem_protocol import FilesystemProtocol


class GCSFilesystem(FilesystemProtocol):
    def __init__(self, hook: GCSHook):
        self.hook = hook

    def read(self, path: str) -> bytes:
        bucket_name, object_name = _get_bucket_and_key_name(path)
        return self.hook.download(bucket_name, object_name)

    def write(self, data: str | bytes | BytesIO, path: str):
        bucket_name, object_name = _get_bucket_and_key_name(path)
        self.hook.upload(bucket_name, object_name, data=data)

    def delete_file(self, path: str):
        bucket_name, object_name = _get_bucket_and_key_name(path)
        self.hook.delete(bucket_name, object_name)

    def create_prefix(self, prefix: str):
        bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
        if not self.hook.bucket_exists(bucket_name):
            self.hook.create_bucket(bucket_name)
        # Create an empty object to represent the prefix
        self.hook.upload(bucket_name, key_prefix, data='')

    def delete_prefix(self, prefix: str):
        bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
        objects = self.hook.list(bucket_name, prefix=key_prefix)
        if objects:
            for object_name in objects:
                self.hook.delete(bucket_name, object_name)

    def check_file(self, path: str) -> bool:
        bucket_name, object_name = _get_bucket_and_key_name(path)
        return self.hook.exists(bucket_name, object_name)

    def check_prefix(self, prefix: str) -> bool:
        bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
        objects = self.hook.list(bucket_name, prefix=key_prefix, max_results=1)
        return bool(objects)

    def list_files(self, prefix: str) -> list[str]:
        bucket_name, key_prefix = _get_bucket_and_key_name(prefix)
        objects = self.hook.list(bucket_name, prefix=key_prefix)
        return [f'{bucket_name}/{object_path}' for object_path in (objects or [])]


def _get_bucket_and_key_name(path: str) -> tuple[str, str]:
    parts = path.split("/")
    return parts[0], "/".join(parts[1:])
