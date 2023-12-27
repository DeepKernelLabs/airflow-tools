import os
from pathlib import Path

import boto3
import pendulum
import pytest
from airflow import DAG


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
