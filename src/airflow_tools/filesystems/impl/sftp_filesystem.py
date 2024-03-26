from io import BytesIO

from airflow.providers.sftp.hooks.sftp import SFTPHook

from airflow_tools.filesystems.filesystem_protocol import FilesystemProtocol


class SFTPFilesystem(FilesystemProtocol):
    def __init__(self, hook: SFTPHook):
        self.hook = hook

    def read(self, path: str) -> bytes:
        conn = self.hook.get_conn()
        out = BytesIO()
        conn.getfo(remotepath=path, fl=out)
        out.seek(0)
        return out.getvalue()

    def write(self, data: str | bytes | BytesIO, path: str):
        conn = self.hook.get_conn()
        if isinstance(data, str):
            data = data.encode()
        if isinstance(data, bytes):
            data = BytesIO(data)
        conn.putfo(fl=data, remotepath=path, confirm=True)

    def delete_file(self, path: str):
        self.hook.delete_file(path)

    def create_prefix(self, prefix: str):
        self.hook.create_directory(prefix)

    def delete_prefix(self, prefix: str):
        for file in self.hook.list_directory(prefix):
            self.hook.delete_file(prefix.rstrip('/') + '/' + file)
        self.hook.delete_directory(prefix)

    def check_file(self, path: str) -> bool:
        return self.hook.isfile(path)

    def check_prefix(self, prefix: str) -> bool:
        return self.hook.isdir(prefix)

    def list_files(self, prefix: str) -> list[str]:
        return self.hook.list_directory(prefix)
