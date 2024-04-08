import logging

import pendulum
from airflow.operators.python import PythonOperator
from airflow.utils.state import TaskInstanceState as TIS

from airflow_tools.providers.filesystem.tasks import branch_filesystem_check_task

logger = logging.getLogger(__file__)


def test_branch_filesystem_check_task_main_branch(
    dag, s3_bucket, s3_resource, s3_client
):
    # Create entity for testing
    s3_resource.Object(s3_bucket, 'source1/entity1/2023-10-01/__SUCCESS__').put(
        Body='{"id":1}\n{"id":2}\n'
    )
    with dag:
        file_check = branch_filesystem_check_task(
            filesystem_conn_id='data_lake_test',
            filesystem_path=s3_bucket + '/source1/entity1/2023-10-01/__SUCCESS__',
            main_branch='main_branch',
            alternative_branch='alternative_branch',
        )
        main_branch = PythonOperator(
            task_id='main_branch', python_callable=lambda: 'main_branch'
        )

        alternative_branch = PythonOperator(
            task_id='alternative_branch', python_callable=lambda: 'alternative_branch'
        )

        file_check >> [main_branch, alternative_branch]

    res = dag.test(execution_date=pendulum.datetime(2023, 10, 1))

    assert res.get_task_instance('main_branch').state == TIS.SUCCESS
    assert res.get_task_instance('alternative_branch').state == TIS.SKIPPED


def test_branch_filesystem_check_task_alternative_branch(dag, s3_bucket):
    # Create entity for testing
    with dag:
        file_check = branch_filesystem_check_task(
            filesystem_conn_id='data_lake_test',
            filesystem_path=s3_bucket + '/source1/entity1/2023-10-01/__SUCCESS__',
            main_branch='main_branch',
            alternative_branch='alternative_branch',
        )
        main_branch = PythonOperator(
            task_id='main_branch', python_callable=lambda: 'main_branch'
        )

        alternative_branch = PythonOperator(
            task_id='alternative_branch', python_callable=lambda: 'alternative_branch'
        )

        file_check >> [main_branch, alternative_branch]

    res = dag.test(execution_date=pendulum.datetime(2023, 10, 1))

    assert res.get_task_instance('main_branch').state == TIS.SKIPPED
    assert res.get_task_instance('alternative_branch').state == TIS.SUCCESS
