import json
import os
from pathlib import Path

import boto3
import pendulum
import pytest
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session


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

    conf.load_test_config()


@pytest.fixture
def dag(load_airflow_test_config):
    with create_session() as session:
        session.query(DagRun).filter(DagRun.dag_id == 'test-dag').delete()
        session.commit()
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
def sqlite_conn(tmp_path, monkeypatch):
    db_path = (tmp_path / 'warehouse.db').absolute()
    monkeypatch.setenv(
        'AIRFLOW_CONN_DATA_WAREHOUSE_SQLITE',
        json.dumps(
            {
                'conn_type': 'sqlite',
                'host': str(db_path),
            }
        ),
    )
    yield BaseHook().get_connection(conn_id='data_warehouse_sqlite')
