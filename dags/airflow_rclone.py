import pendulum
from airflow.decorators import dag

from common.constant import DEFAULT_ARGS
from operators.rclone import RCloneOperator


@dag(
    dag_id="fs_sync_rclone",
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["file-system", "sync"]
)
def rclone():
    RCloneOperator(
        task_id="rclone",
        source_name="sourcesftp",
        source_conn_id="sftp_source_conn",
        source_conn_type="sftp",
        source_path="{{ var.value.fs_sync_source_path }}",
        target_name="targetsftp",
        target_conn_id="sftp_target_conn",
        target_conn_type="sftp",
        target_path="{{ var.value.fs_sync_target_path }}",
    )

dag = rclone()
