import uuid
from contextlib import closing

import pendulum
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from airflow_tools.providers.partitions.operators.partitions import (
    SkipOrReplacePartition,
)

DATA_INTERVAL_START = pendulum.datetime(2023, 10, 1)
DATA_INTERVAL_END = pendulum.datetime(2023, 10, 2)


def test_skip_or_replace_partition(dag, s3_bucket, s3_resource, sqlite_conn):
    with sqlite_conn.get_hook().get_conn() as conn, closing(conn.cursor()) as cursor:
        cursor.execute('CREATE TABLE entity1 (id INTEGER PRIMARY KEY, ds STRING)')
    TASK_ID = 'test_skip_or_replace_partition'
    with dag:
        SkipOrReplacePartition(
            task_id=TASK_ID,
            data_lake_conn_id='data_lake_test',
            data_warehouse_conn_id=sqlite_conn.conn_id,
            data_lake_partition_path=s3_bucket + '/source1/entity1/{{ ds }}/',
            data_warehouse_schema='source1',
            data_warehouse_table='entity1',
        )

    dagrun = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
        run_id=str(uuid.uuid4()),
    )
    ti = dagrun.get_task_instance(task_id=TASK_ID)
    ti.task = dag.get_task(task_id=TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS

    s3_resource.Object(s3_bucket, 'source1/entity1/2023-10-01/part0001.jsonl').put(
        Body=b'Hello World!'
    )
    s3_resource.Object(s3_bucket, 'source1/entity1/2023-10-01/__SUCCESS__').put(
        Body=b''
    )

    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SKIPPED
