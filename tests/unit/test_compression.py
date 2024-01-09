from typing import Union

import pytest

from airflow_tools.compression_utils import compress, decompress


@pytest.mark.parametrize('compression', ['gzip', 'zip', None])
def test_compression(compression: Union[str, None]):
    data = b'hello world'
    assert decompress(compression, compress(compression, data)) == data
