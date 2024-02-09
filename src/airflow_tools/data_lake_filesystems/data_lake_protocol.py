from io import BytesIO
from typing import Protocol


class DataLakeProtocol(Protocol):
    def write(self, data: str | bytes | BytesIO, path: str):
        ...

    def delete_prefix(self, prefix: str):
        ...
