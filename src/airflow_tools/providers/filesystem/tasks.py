import logging

from airflow.decorators import branch_task
from airflow.hooks.base import BaseHook

from airflow_tools.filesystems.filesystem_factory import FilesystemFactory

logger = logging.getLogger(__file__)


@branch_task
def branch_filesystem_check_task(
    filesystem_conn_id: str,
    filesystem_path: str,
    main_branch: str,
    alternative_branch: str,
) -> bool:
    filesystem_protocol = FilesystemFactory.get_data_lake_filesystem(
        connection=BaseHook.get_connection(filesystem_conn_id),
    )
    logger.info(f"Trying to check: {filesystem_path}")
    if (
        filesystem_protocol.check_prefix(filesystem_path)
        if filesystem_path.endswith("/")
        else filesystem_protocol.check_file(filesystem_path)
    ):
        return main_branch
    else:
        return alternative_branch
