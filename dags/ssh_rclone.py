import pendulum
from airflow.decorators import dag

from common.constant import DEFAULT_ARGS
from operators.ssh_rclone import SSHRCloneOperator


@dag(
    dag_id="fs_sync_ssh_rclone",
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["file-system", "sync", "ssh"]
)
def ssh_rclone():
    SSHRCloneOperator(
        task_id="ssh_rclone",
        ssh_conn_id="sftp_source_conn",
        source_name="",
        source_path="{{ var.value.fs_sync_source_path }}",
        target_name="target",
        target_config={
            "type": "sftp",
            "host": "{{ conn.sftp_target_conn.host }}",
            "user": "{{ conn.sftp_target_conn.login }}",
            "pass": "{{ conn.sftp_target_conn.password }}",
        },
        target_path="{{ var.value.fs_sync_target_path }}",
        secret_target_fields=["pass"],
        conn_timeout=None,
        cmd_timeout=None,
        skip_on_exit_code=None,
    )

dag = ssh_rclone()
