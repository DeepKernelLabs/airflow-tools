from io import BytesIO
from typing import Protocol


class FilesystemProtocol(Protocol):
    def read(self, path: str) -> bytes:
        ...

    def write(self, data: str | bytes | BytesIO, path: str):
        ...

    def delete_file(self, path: str):
        ...

    def create_prefix(self, prefix: str):
        ...

    def delete_prefix(self, prefix: str):
        ...

    def check_file(self, path: str) -> bool:
        ...

    def check_prefix(self, prefix: str) -> bool:
        ...

    def list_files(self, prefix: str) -> list[str]:
        ...
