import json
import os

import boto3

from airflow_tools.providers.http_to_data_lake.operators.http_to_data_lake import (
    HttpToDataLake,
)


def clear_bucket(bucket_name):
    """Delete all objects in the specified S3 bucket."""
    s3 = boto3.resource('s3', endpoint_url='http://localhost:9090')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()


def get_object_content(bucket_name, key):
    """Fetch the content of an object from the specified S3 bucket and key."""
    s3 = boto3.client('s3', endpoint_url='http://localhost:9090')
    response = s3.get_object(Bucket=bucket_name, Key=key)
    return response['Body'].read().decode('utf-8')


def test_http_to_data_lake(load_airflow_test_config):
    """The endpoint was created with mocky.io and returns the following response:
    Mock URL: https://run.mocky.io/v3/9ebfc446-54ad-41a1-ab2f-5cd710d1324a
    Response: {"response": [{"a": "foo", "b": 1}, {"a": "bar", "b": 2}]}
    Secrete Delete Link: https://designer.mocky.io/manage/delete/9ebfc446-54ad-41a1-ab2f-5cd710d1324a/lLGaDThvoQdaQJ2FzYrkDRMeHxo1UfRdjASr
    WARNING: Do not visit the secret delete link unless you want to delete this test
    """
    clear_bucket('data_lake')

    os.environ['AIRFLOW_CONN_HTTP_TEST'] = json.dumps(
        {
            'conn_type': 'http',
            'host': 'https://run.mocky.io/v3',
        }
    )
    operator = HttpToDataLake(
        task_id='test_http_to_data_lake',
        http_conn_id='http_test',
        data_lake_conn_id='data_lake_test',
        data_lake_path='data_lake/source1/entity1/{{ ds }}/',
        endpoint='/9ebfc446-54ad-41a1-ab2f-5cd710d1324a',
        method='GET',
        jmespath_expression='response',
    )
    operator.execute({'ds': '2023-10-01'})

    # content = get_object_content('data_lake', 'data_lake/source1/entity1/2023-10-01/')
    content = get_object_content('data_lake', 'source1/entity1/{{ ds }}/part0001.jsonl')
    assert content == '{"a":"foo","b":1}\n{"a":"bar","b":2}\n'
