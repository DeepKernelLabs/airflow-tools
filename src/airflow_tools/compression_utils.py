import gzip
import zipfile
from io import BytesIO
from typing import Literal, Union

DEFAULT_ZIP_FILENAME = 'file.zip'
CompressionOptions = Union[Literal['infer', 'gzip', 'bz2', 'zip', 'xz', 'zstd'], None]


def gzip_data(data: bytes) -> bytes:
    out = BytesIO()
    with BytesIO() as out:
        with gzip.GzipFile(fileobj=out, mode="wb") as f:
            f.write(data)
        out.seek(0)
        return out.getvalue()


def zip_data(data: bytes, filename: str = DEFAULT_ZIP_FILENAME):
    with BytesIO() as out:
        with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr(filename, data)

        out.seek(0)
        return out.getvalue()


def compress(compression: 'CompressionOptions', data: bytes) -> bytes:
    if compression is None:
        return data
    if compression == 'gzip':
        return gzip_data(data)
    if compression == 'zip':
        return zip_data(data)
    raise NotImplementedError(f'Unknown compression: {compression}')


def ungzip_data(compressed_data: bytes) -> bytes:
    with BytesIO(compressed_data) as input_stream:
        with gzip.GzipFile(fileobj=input_stream, mode="rb") as f:
            return f.read()


def unzip_data(compressed_data: bytes, filename: str = DEFAULT_ZIP_FILENAME) -> bytes:
    with BytesIO(compressed_data) as input_stream:
        with zipfile.ZipFile(input_stream) as zip_file:
            with zip_file.open(filename) as file:
                return file.read()


def decompress(compression: 'CompressionOptions', data: bytes) -> bytes:
    if compression is None:
        return data
    if compression == 'gzip':
        return ungzip_data(data)
    if compression == 'zip':
        return unzip_data(data)
    raise NotImplementedError(f'Unknown compression: {compression}')
