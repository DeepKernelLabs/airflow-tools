"""
We need the unused imports to fix this error:

=========================== short test summary info ============================
FAILED tests/integration/test_http_to_data_lake.py::test_http_to_data_lake - sqlalchemy.exc.NoReferencedTableError: Foreign key associated with column 'dag_run_note.user_id' could not find table 'ab_user' with which to generate a foreign key to target column 'id'
============= 1 failed, 10 passed, 9 warnings in 98.08s (0:01:38) ==============

TODO: Remove this in the future once airflow fixes it
"""
import os
from pathlib import Path

import boto3
import pendulum
import pytest
from airflow import DAG
from airflow.auth.managers.fab.models import User  # noqa # type: ignore
from airflow.models.dagrun import DagRunNote  # noqa


@pytest.fixture
def project_path() -> Path:
    return Path(__file__).parent.parent.absolute()


@pytest.fixture
def source_path(project_path) -> Path:
    return project_path / 'src'


@pytest.fixture
def tests_path(project_path) -> Path:
    return project_path / 'tests'


@pytest.fixture
def load_airflow_test_config() -> Path:
    from airflow.configuration import conf

    return conf.load_test_config()


@pytest.fixture
def dag(load_airflow_test_config):
    with DAG(
        dag_id='test-dag',
        schedule="@daily",
        start_date=pendulum.datetime(2020, 1, 1),
        catchup=False,
    ) as dag:
        yield dag


@pytest.fixture
def s3_bucket(s3_resource):
    """Delete all objects in the specified S3 bucket."""
    bucket_name = os.environ['TEST_BUCKET']
    bucket = s3_resource.Bucket(bucket_name)
    bucket.objects.all().delete()
    return bucket_name


@pytest.fixture
def s3_resource():
    endpoint_url = os.environ['S3_ENDPOINT_URL']
    return boto3.resource('s3', endpoint_url=endpoint_url)


@pytest.fixture
def s3_client():
    endpoint_url = os.environ['S3_ENDPOINT_URL']
    return boto3.client('s3', endpoint_url=endpoint_url)
