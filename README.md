# Airflow Tools
<div align="center">
  <!-- Logo -->
  <img src="./media/images/logo.webp" alt="logo" width="400"/>

  <!-- Add some space between the logo and badges -->
  <br/>

  <!-- Badges -->
  <a href="https://github.com/DeepKernelLabs/airflow-tools/actions?query=branch%3Amain">
    <img src="https://github.com/DeepKernelLabs/airflow-tools/actions/workflows/lint-and-test.yml/badge.svg?branch=main" alt="Badge 1"/>
  </a>
  <a href="https://opensource.org/licenses/Apache-2.0">
    <img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="Badge 2"/>
  </a>
  <a href="https://img.shields.io/badge/python-3.10%20%7C%203.11-blue">
    <img src="https://img.shields.io/badge/python-3.10%20%7C%203.11-blue" alt="Badge 3"/>
  </a>

  <br/><br/>
</div>

Collection of Operators, Hooks and utility functions aimed at facilitating ELT pipelines.

## Overview

This is an opinionated library focused on ELT, which means that our goal is to facilitate loading data from various data sources into a data lake, as well as loading from a data lake to a data warehouse and running transformations inside a data warehouse. 

Airflow's operators notoriously suffer from an NxM problem, where if you have N data sources and M destinations you end up with NxM different operators (FTPToS3, S3ToFTP, PostgresToS3, FTPToPostgres etc.). We aim to mitigate this issue in two ways:

1. **ELT focused:** cuts down on the number of possible sources and destinations, as we always want to do `source` -> `data lake` -> `data warehouse`.
2. **Building common interfaces:** where possible we want to treat similar data sources in the same way. Airflow recently has done a good job at this by deprecating all specific SQL operators (`PostgresOperator`, `MySQLOperator`, etc.) in favour of a more generic `SQLExecuteQueryOperator` that works with any hook compatible with the dbapi2 interface. We take this philosophy and apply it any time we can, like providing a unified interface for all filesystem data sources that then enables us to have much more generic operators like `SQLToFilesystem`, `FilesystemToFilesystem`.

## Filesystem Interface/Protocol

We provide a thin wrapper over many hooks for filesystem data sources. These wrappers use the hook's specific methods to implement some common methods that we then use inside the operators without needing to worry about the hook's specific type. For now we provide support for the following filesystem hooks, some of them native or that belong to other providers and others implemented in this library:

- WasbHook (Blob Storage/ADLS)
- S3Hook (S3)
- SFTPHook (SFTP)
- FSHook (Local Filesystem)
- AzureFileShareServicePrincipalHook (Azure Fileshare with support for service principal authentication)
- AzureDatabricksVolumeHook (Unity Catalog Columes)

Operators can create the correct hook at runtime by passing a connection ID with a connection type of `aws` or `adls`. Example code:

```python
conn = BaseHook.get_connection(conn_id)
hook = conn.get_hook()
```

## Operators
### HTTP to Filesystem (Data Lake)

Creates a
Example usage:

```python
HttpToFilesystem(
    task_id='test_http_to_data_lake',
    http_conn_id='http_test',
    data_lake_conn_id='data_lake_test',
    data_lake_path=s3_bucket + '/source1/entity1/{{ ds }}/',
    endpoint='/api/users',
    method='GET',
    jmespath_expression='data[:2].{id: id, email: email}',
)
```

#### JMESPATH expressions
APIs often return the response we are interested in wrapped in a key. JMESPATH expressions are a query language that we can use to select the response we are interested in. You can find more information on JMESPATH expressions and test them [here](https://jmespath.org/).

The above expression selects the first two objects inside the key data, and then only the `id` and `email` attributes in each object. An example response can be found [here](https://reqres.in/api/users).

## Tests
### Integration tests
To guarantee that the library works as intended we have an integration test that attempts to install it in a fresh virtual environment, and we aim to have a test for each Operator.

#### Running integration tests locally
The `lint-and-test.yml` [workflow](.github/workflows/lint-and-test.yml) sets up the necessary environment variables, but if you want to run them locally you will need the following environment variables:

```shell
AIRFLOW_CONN_DATA_LAKE_TEST='{"conn_type": "aws", "extra": {"endpoint_url": "http://localhost:9090"}}'
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_DEFAULT_REGION=us-east-1
TEST_BUCKET=data_lake
S3_ENDPOINT_URL=http://localhost:9090

AIRFLOW_CONN_DATA_LAKE_TEST='{"conn_type": "aws", "extra": {"endpoint_url": "http://localhost:9090"}}' AIRFLOW_CONN_SFTP_TEST='{"conn_type": "sftp", "host": "localhost", "port": 22, "login": "test_user", "password": "pass"' AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY TEST_BUCKET=data_lake S3_ENDPOINT_URL=http://localhost:9090 poetry run pytest tests/ --doctest-modules --junitxml=junit/test-results.xml --cov=com --cov-report=xml --cov-report=html
AIRFLOW_CONN_SFTP_TEST='{"conn_type": "sftp", "host": "localhost", "port": 22, "login": "test_user", "password": "pass"}'
```

And you also need to run [Adobe's S3 mock container](https://github.com/adobe/S3Mock) like this:

```shell
docker run --rm -p 9090:9090 -e initialBuckets=data_lake -e debug=true -t adobe/s3mock
```

and the SFTP container like this:

```shell
docker run -p 22:22 -d atmoz/sftp test_user:pass:::root_folder
```


### Notifications

#### Slack (incoming webhook)

If your or your team are using slack, you can send and receive notifications about failed dags using `dag_failure_slack_notification_webhook` method
(in `notifications.slack.webhook`). You need to create a new Slack App and enable the "Incoming Webhooks". More info about sending messages using
Slack Incoming Webhooks [here](https://api.slack.com/messaging/webhooks).

You need to create a new Airflow connection with the name `SLACK_WEBHOOK_NOTIFICATION_CONN` (or `AIRFLOW_CONN_SLACK_WEBHOOK_NOTIFICATION_CONN`
if you are using environment variables.)

Default message will have the format below:

![image](https://github.com/DeepKernelLabs/airflow-tools/assets/152852247/52a5bf95-21bc-4c3b-8093-79953c0c5d61)

But you can custom this message providing the below parameters:

* **_text (str)[optional]:_** the main message will appear in the notification. If you provide your slack block will be ignored.
* **_blocks (dict)[optional]:_** you can provide your custom slack blocks for your message.
* **_include_blocks (bool)[optional]:_** indicates if the default block have to be used. If you provide your own blocks will be ignored.
* **_source (typing.Literal['DAG', 'TASK'])[optional]:_** source of the failure (dag or task). Default: `DAG`.
* **_image_url: (str)[optional]_** image url for you notification (`accessory`). You can use `AIRFLOW_TOOLS__SLACK_NOTIFICATION_IMG_URL` instead.

##### Example of use in a Dag

```python
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow_tools.notifications.slack.webhook import (
    dag_failure_slack_notification_webhook,    # <--- IMPORT
)

with DAG(
    "slack_notification_dkl",
    description="Slack notification on fail",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
    on_failure_callback=dag_failure_slack_notification_webhook(),  # <--- HERE
) as dag:

    t = BashOperator(
        task_id="failing_test",
        depends_on_past=False,
        bash_command="exit 1",
        retries=1,
    )


if __name__ == "__main__":
    dag.test()
```

You can used only in a task providing the parameter `source='TASK'`:

```python
    t = BashOperator(
        task_id="failing_test",
        depends_on_past=False,
        bash_command="exit 1",
        retries=1,
        on_failure_callback=dag_failure_slack_notification_webhook(source='TASK')
    )
```

You can add a custom message (ignoring the slack blocks for a formatted message):

```python
with DAG(
    ...
    on_failure_callback=dag_failure_slack_notification_webhook(
        text='The task {{ ti.task_id }} failed',
        include_blocks=False
    ),
) as dag:
```

Or you can pass your own Slack blocks:

```python
custom_slack_blocks = {
    "type": "section",
    "text": {
        "type": "mrkdwn",
        "text": "<https://api.slack.com/reference/block-kit/block|This is an example using custom Slack blocks>"
    }
}

with DAG(
    ...
    on_failure_callback=dag_failure_slack_notification_webhook(
        blocks=custom_slack_blocks
    ),
) as dag:
```
