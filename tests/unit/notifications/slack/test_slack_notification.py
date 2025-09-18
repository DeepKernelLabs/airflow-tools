import pytest
from airflow import DAG
from airflow.operators.bash import BashOperator

from airflow_toolkit.notifications.slack import webhook as slack_webhook


def test_webhook_slack_notifications_on_fail(mocker):
    slack_mock = mocker.patch.object(
        slack_webhook, "dag_failure_slack_notification_webhook"
    )

    with DAG("test_slack_notification_on_fail_dag") as dag:
        BashOperator(
            task_id="failing_task",
            bash_command="exit 1",
            on_failure_callback=slack_webhook.dag_failure_slack_notification_webhook(),
        )

    dag.test()

    slack_mock.assert_called_once_with()


@pytest.mark.parametrize(
    "source, expected_text",
    [
        (
            slack_webhook.Source.DAG,
            "{% for t in dag_run.get_task_instances(state='failed') %}'{{ t.task_id }}', {% endfor %}",
        ),
        (slack_webhook.Source.TASK, "{{ task.task_id }}"),
        ("unkown source", '"unknown source"'),
    ],
)
def test_webhook_block_blocks_format_by_source(source: str, expected_text: str):
    blocks = slack_webhook._get_message_blocks(
        text=f"test_{source}", image_url="http://localhost/image.png", source=source
    )
    failed_task_info = blocks[0]["text"]["text"]
    assert expected_text in failed_task_info
