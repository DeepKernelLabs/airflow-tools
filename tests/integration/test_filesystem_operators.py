import pendulum
import pytest
from airflow.operators.python import PythonOperator
from botocore.exceptions import ClientError

from airflow_tools.providers.filesystem.operators.filesystem import (
    FilesystemCheckOperator,
    FilesystemDeleteOperator,
)


def test_filesystem_delete_operator(dag, s3_bucket, s3_resource, s3_client):
    # Create entity for testing
    s3_resource.Object(s3_bucket, 'source1/entity1/2023-10-01/part0001.jsonl').put(
        Body='{"id":1}\n{"id":2}\n'
    )

    # Pre-condition: Entity should exist before delete, if not it will raise an error
    s3_client.head_object(
        Bucket=s3_bucket, Key='source1/entity1/2023-10-01/part0001.jsonl'
    )

    with dag:
        FilesystemDeleteOperator(
            task_id='test_data_lake',
            filesystem_conn_id='data_lake_test',
            filesystem_path=s3_bucket + '/source1/entity1/{{ ds }}/',
        )
    dag.test(execution_date=pendulum.datetime(2023, 10, 1))

    # Entity should not exist after delete
    with pytest.raises(ClientError):
        s3_client.head_object(
            Bucket=s3_bucket, Key='source1/entity1/2023-10-01/part0001.jsonl'
        )


def print_res(task_id, expected_result, **context):
    print('\n\n\n-------------------------')
    print('Task id: ', task_id)
    print('Expected result: ', expected_result)
    print('Real result: ', context['ti'].xcom_pull(task_ids=task_id))
    print('-------------------------\n\n\n')
    assert context['ti'].xcom_pull(task_ids=task_id) == expected_result


def test_filesystem_check_operator(dag, s3_bucket, s3_resource, s3_client):
    # Create entity for testing
    s3_resource.Object(s3_bucket, 'source1/entity1/2023-10-01/part0001.jsonl').put(
        Body='{"id":1}\n{"id":2}\n'
    )
    s3_resource.Object(s3_bucket, 'source1/entity1/2023-10-01/__SUCCESS__').put(Body='')

    # Pre-condition: Entity should exist before delete, if not it will raise an error
    s3_client.head_object(
        Bucket=s3_bucket, Key='source1/entity1/2023-10-01/part0001.jsonl'
    )

    with dag:
        # Check correct prefix, no extra file
        res_correct_prefix = FilesystemCheckOperator(
            task_id='test_data_lake_correct_prefix',
            filesystem_conn_id='data_lake_test',
            filesystem_path=s3_bucket + '/source1/entity1/{{ ds }}/',
        )

        check_res_correct_prefix = PythonOperator(
            task_id='check_res_correct_prefix',
            python_callable=print_res,
            provide_context=True,
            op_args=['test_data_lake_correct_prefix', True],
        )

        # Check incorrect prefix, no extra file
        res_incorrect_prefix = FilesystemCheckOperator(
            task_id='test_data_lake_incorrect_prefix',
            filesystem_conn_id='data_lake_test',
            filesystem_path=s3_bucket + '/source1/entity2/{{ ds }}/',
        )

        check_res_incorrect_prefix = PythonOperator(
            task_id='check_res_incorrect_prefix',
            python_callable=print_res,
            provide_context=True,
            op_args=['test_data_lake_incorrect_prefix', False],
        )

        # Check correct prefix, extra file
        res_correct_prefix_extra_file = FilesystemCheckOperator(
            task_id='test_data_lake_correct_prefix_extra_file',
            filesystem_conn_id='data_lake_test',
            filesystem_path=s3_bucket + '/source1/entity1/{{ ds }}/',
            check_specific_filename='__SUCCESS__',
        )

        check_res_correct_prefix_extra_file = PythonOperator(
            task_id='check_correct_extra_file',
            python_callable=print_res,
            provide_context=True,
            op_args=['test_data_lake_correct_prefix_extra_file', True],
        )

        # Check correct prefix, incorrect extra file
        res_prefix_incorrect_extra_file = FilesystemCheckOperator(
            task_id='test_data_lake_prefix_incorrect_extra_file',
            filesystem_conn_id='data_lake_test',
            filesystem_path=s3_bucket + '/source1/entity1/{{ ds }}/',
            check_specific_filename='__FAILURE__',
        )

        check_res_incorrect_extra_file = PythonOperator(
            task_id='check_res_incorrect_extra_file',
            python_callable=print_res,
            provide_context=True,
            op_args=['test_data_lake_prefix_incorrect_extra_file', False],
        )

        res_correct_prefix >> check_res_correct_prefix
        res_incorrect_prefix >> check_res_incorrect_prefix
        res_correct_prefix_extra_file >> check_res_correct_prefix_extra_file
        res_prefix_incorrect_extra_file >> check_res_incorrect_extra_file

    dag.test(execution_date=pendulum.datetime(2023, 10, 1))

    # Entity should not exist after delete
