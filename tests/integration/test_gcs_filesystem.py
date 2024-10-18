import uuid

from airflow.hooks.base import BaseHook

from airflow_tools.filesystems.filesystem_factory import FilesystemFactory
from airflow_tools.filesystems.impl.google_cloud_storage_filesystem import GCSFilesystem


def test_gcs_filesystem():
    """Expects an environment variable set to a GCS test bucket.
    AIRFLOW_CONN_GCP_DATA_LAKE_TEST='{"conn_type": "google_cloud_platform", "extra": {"key_path": "/.../keyfile.json"}}'"""
    conn = BaseHook.get_connection("gcp_data_lake_test")
    gcs_fs = FilesystemFactory.get_data_lake_filesystem(conn)

    assert isinstance(gcs_fs, GCSFilesystem)

    test_prefix = f'data-lake-test-ksa-insights/tests/{uuid.uuid4()}/'
    test_file_path = f'{test_prefix}foo.txt'
    test_text = 'Hello world!'
    gcs_fs.write(test_text, test_file_path)

    assert gcs_fs.check_file(test_file_path)

    files = gcs_fs.list_files(test_prefix)
    assert len(files) == 1
    assert files[0] == test_file_path

    assert gcs_fs.read(test_file_path).decode() == test_text

    assert gcs_fs.check_prefix(test_prefix)

    gcs_fs.delete_file(test_file_path)

    assert not gcs_fs.check_file(test_file_path)
    assert not gcs_fs.check_prefix(test_prefix)
