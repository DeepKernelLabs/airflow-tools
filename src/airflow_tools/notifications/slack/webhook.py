import os
import typing

from airflow.configuration import conf
from airflow.providers.slack.notifications.slack_webhook import (
    send_slack_webhook_notification,
)

SLACK_WEBHOOK_CONN = 'slack_webhook_notification_conn'
DEFAULT_DAG_MSG = ':red_circle: *DAG _\"{{dag.dag_id}}\"_ failed*'
DEFAULT_TASK_MSG = ':red_circle: *Task _\"{{task.task_id}}\"_ failed*'
SLACK_NOTIFICATION_IMG_URL = os.getenv('AIRFLOW_TOOLS__SLACK_NOTIFICATION_IMG_URL')


class Source:
    DAG = 'DAG'
    TASK = 'TASK'


def _get_message_blocks(
    text: str, image_url: str, source: str
) -> list[dict[str, typing.Any]]:
    base_url = conf.get('webserver', 'BASE_URL')
    dag_execution_logs_url = (
        base_url + '/dags/{{ dag.dag_id }}/grid?'
        'dag_run_id={{ run_id | urlencode }}&'
        'task_id={{ task.task_id }}&tab=logs'
    )

    match source:
        case Source.DAG:
            failed_tasks = "{% for t in dag_run.get_task_instances(state='failed') %}'{{ t.task_id }}', {% endfor %}"
        case Source.TASK:
            failed_tasks = "{{ task.task_id }}"
        case _:
            failed_tasks = '"unknown source"'

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
                "> Failed tasks:    [" + failed_tasks + "]\n"
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
    text: str = '',
    blocks: typing.List[dict[str, typing.Any]] | None = None,
    include_blocks: bool = True,
    source: typing.Literal['DAG', 'TASK'] = Source.DAG,
    image_url: str | None = SLACK_NOTIFICATION_IMG_URL,
) -> typing.Callable:

    """
    Sends a Slack notification for DAG failure using a webhook.

    Args:
        conn_id (str) [optional]: The connection ID for the Slack webhook.
        text (str) [optional]: The text message to be sent in the Slack notification. Default:
        blocks (dict) [optional]: The blocks to be included in the Slack notification.
        include_blocks (bool) [optional]: Whether to include default message blocks if `blocks` is not provided.
        source (typing.Literal['DAG', 'TASK']) [optional]: The source of the failure (DAG or TASK). Default: 'DAG'.
        image_url (str) [optional]: The URL of the image to be included in the Slack notification.

    Returns:
        typing.Callable: A callable object that sends the Slack notification.

    """

    if not text:
        text = DEFAULT_DAG_MSG if source == Source.DAG else DEFAULT_TASK_MSG

    if not blocks and include_blocks:
        blocks = _get_message_blocks(text=text, image_url=image_url, source=source)

    return send_slack_webhook_notification(
        slack_webhook_conn_id=conn_id, text=text, blocks=blocks
    )
