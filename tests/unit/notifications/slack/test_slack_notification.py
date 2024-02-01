from airflow import DAG
from airflow.operators.bash import BashOperator

from src.airflow_tools.notifications.slack import webhook as slack_webhook


def test_webhook_slack_notifications_on_fail(mocker):
    slack_mock = mocker.patch.object(
        slack_webhook, 'dag_failure_slack_notification_webhook'
    )

    with DAG('test_slack_notification_on_fail_dag') as dag:
        BashOperator(
            task_id='failing_task',
            bash_command='exit 1',
            on_failure_callback=slack_webhook.dag_failure_slack_notification_webhook(),
        )

    dag.test()

    slack_mock.assert_called_once_with()
