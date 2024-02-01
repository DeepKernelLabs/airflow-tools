# Airflow Tools
![Workflow](https://github.com/DeepKernelLabs/airflow-tools/actions/workflows/lint-and-test.yml/badge.svg?branch=main)

Collection of Operators, Hooks and utility functions aimed at facilitating ELT pipelines.

## Data Lake Facade
The Data Lake Facade serves as an abstracion over different Hooks that can be used as a backend such as:
- Azure Data Lake Storage (ADLS)
- Simple Storage Service (S3)

Operators can create the correct hook at runtime by passing a connection ID with a connection type of `aws` or `adls`. Example code:

```python
conn = BaseHook.get_connection(conn_id)
hook = conn.get_hook()
```

## Operators
### HTTP to Data Lake

Creates a
Example usage:

```python
HttpToDataLake(
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

AIRFLOW_CONN_DATA_LAKE_TEST='{"conn_type": "aws", "extra": {"endpoint_url": "http://localhost:9090"}}' AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY TEST_BUCKET=data_lake S3_ENDPOINT_URL=http://localhost:9090 poetry run pytest tests/ --doctest-modules --junitxml=junit/test-results.xml --cov=com --cov-report=xml --cov-report=html
```

And you also need to run [Adobe's S3 mock container](https://github.com/adobe/S3Mock) like this:

```shell
docker run --rm -p 9090:9090 -e initialBuckets=data_lake -e debug=true -t adobe/s3mock
```


### Notifications

#### Slack (incoming webhook)

If your or your team are using slack, you can send and receive notifications about failed dags using `dag_failure_slack_notification_webhook` method
(in `notifications.slack.webhook`). You need to create a new Slack App and enable the "Incoming Webhooks". More info about sending messages using
Slack Incoming Webhooks [here](https://api.slack.com/messaging/webhooks).

You need to create a new Airflow connection with the name `SLACK_WEBHOOK_NOTIFICATION_CONN` (or `AIRFLOW_CONN_SLACK_WEBHOOK_NOTIFICATION_CONN`
if you are using environment variables.)

Default message will have the format below:

![Slack notification snapshot](https://private-user-images.githubusercontent.com/152852247/300061280-e9b4b8f1-6741-4d77-85a4-358645500c15.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MDY1MTE2OTUsIm5iZiI6MTcwNjUxMTM5NSwicGF0aCI6Ii8xNTI4NTIyNDcvMzAwMDYxMjgwLWU5YjRiOGYxLTY3NDEtNGQ3Ny04NWE0LTM1ODY0NTUwMGMxNS5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BS0lBVkNPRFlMU0E1M1BRSzRaQSUyRjIwMjQwMTI5JTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI0MDEyOVQwNjU2MzVaJlgtQW16LUV4cGlyZXM9MzAwJlgtQW16LVNpZ25hdHVyZT0zODQ2N2M4NWE2MmFjMjFiMjZjZDVmYjU4YWE2YzBlZDhkODgwMmViNzVlYmFjY2Y4NzM0YWNlNTMzZDA5MzYxJlgtQW16LVNpZ25lZEhlYWRlcnM9aG9zdCZhY3Rvcl9pZD0wJmtleV9pZD0wJnJlcG9faWQ9MCJ9._uVapOD4BjuSqr8COZGfMQZZvNxcMsU3JEOXlhoDeyc)

But you can custom this message providing the below parameters:

* **_text (str)[optional]:_** the main message will appear in the notification. If you provide your slack block will be ignored.
* **_blocks (dict)[optional]:_** you can provide your custom slack blocks for your message.
* **_include_blocks (bool)[optional]:_** indicates if the default block have to be used. If you provide your own blocks will be ignored.
* **_image_url: (str)[optional]_** image url for you notification (`accessory`). You can use `AIRFLOW_TOOLS__SLACK_NOTIFICATION_IMG_URL` instead.
