import os
import typing

from airflow.configuration import conf
from airflow.providers.slack.notifications.slack_webhook import (
    send_slack_webhook_notification,
)

SLACK_WEBHOOK_CONN = 'slack_webhook_notification_conn'
DEFAULT_MSG = ':red_circle: *DAG _\"{{dag.dag_id}}\"_ failed*'
SLACK_NOTIFICATION_IMG_URL = os.getenv('AIRFLOW_TOOLS__SLACK_NOTIFICATION_IMG_URL')


def _get_message_blocks(text: str, image_url: str) -> list[dict]:
    base_url = conf.get('webserver', 'BASE_URL')
    dag_execution_logs_url = (
        base_url + '/dags/{{ dag.dag_id }}/grid?'
        'dag_run_id={{ run_id | urlencode }}&'
        'task_id={{ task.task_id }}&tab=logs'
    )

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": str(text) + ":\n"
                "```"
                "> Run_id:          {{ run_id }}\n"
                "> Execution_date:  {{ dag_run.execution_date }}\n"
                "> Queued_at:       {{ dag_run.queued_at }}\n"
                "> Failed tasks:    [{% for t in dag_run.get_task_instances(state='failed') %}'{{ t.task_id }}', {% endfor %}]\n"
                "> Host:            " + str(base_url) + "  \n"
                "```",
            },
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*More info (logs):*"},
            "accessory": {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "Check in airflow webserver",
                    "emoji": True,
                },
                "value": "airflow",
                "url": str(dag_execution_logs_url),
                "action_id": "button-action",
            },
        },
        {"type": "divider"},
    ]

    if image_url:
        blocks[0]["accessory"] = {
            "type": "image",
            "image_url": str(image_url),
            "alt_text": "Airflow notification (via Slack incoming webhook)",
        }

    return blocks


def dag_failure_slack_notification_webhook(
    conn_id: str = SLACK_WEBHOOK_CONN,
    text: str = DEFAULT_MSG,
    blocks: dict = None,
    include_blocks: bool = True,
    image_url: str = SLACK_NOTIFICATION_IMG_URL,
) -> typing.Callable:

    if not blocks and include_blocks:
        blocks = _get_message_blocks(text=text, image_url=image_url)

    return send_slack_webhook_notification(
        slack_webhook_conn_id=conn_id, text=text, blocks=blocks
    )
