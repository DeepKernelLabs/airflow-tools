import pendulum
import pytest
from botocore.exceptions import ClientError

from airflow_tools.providers.data_lake.operators.data_lake import DataLakeDeleteOperator


def test_data_lake_delete_operator(dag, s3_bucket, s3_resource, s3_client):
    # Create entity for testing
    s3_resource.Object(s3_bucket, 'source1/entity1/2023-10-01/part0001.jsonl').put(
        Body='{"id":1}\n{"id":2}\n'
    )

    # Pre-condition: Entity should exist before delete, if not it will raise an error
    s3_client.head_object(
        Bucket=s3_bucket, Key='source1/entity1/2023-10-01/part0001.jsonl'
    )

    with dag:
        DataLakeDeleteOperator(
            task_id='test_data_lake',
            data_lake_conn_id='data_lake_test',
            data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
        )
    dag.test(execution_date=pendulum.datetime(2023, 10, 1))

    # Entity should not exist after delete
    with pytest.raises(ClientError):
        s3_client.head_object(
            Bucket=s3_bucket, Key='source1/entity1/2023-10-01/part0001.jsonl'
        )
