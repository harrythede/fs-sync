import pendulum
from airflow.decorators import dag
from airflow.providers.ssh.operators.ssh import SSHOperator

from common.constant import DEFAULT_ARGS


@dag(
    dag_id="fs_sync_rclone",
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["file-system", "synchronization"]
)
def sync_rclone():

    SSHOperator(
        task_id="rclone",
        ssh_conn_id="sftp_source_conn",
        command="RCLONE_CONFIG_SOURCE_PASS=$(rclone obscure ${RCLONE_CONFIG_SOURCE_PASS});"
                "RCLONE_CONFIG_TARGET_PASS=$(rclone obscure ${RCLONE_CONFIG_TARGET_PASS});"
                "rclone copy source:{{ var.value.sync_path }}/ target:{{ var.value.sync_path }} --metadata --progress",
        environment={
            "RCLONE_CONFIG_SOURCE_TYPE": "sftp",
            "RCLONE_CONFIG_SOURCE_HOST": "{{ conn.sftp_source_conn.host }}",
            "RCLONE_CONFIG_SOURCE_USER": "{{ conn.sftp_source_conn.login }}",
            "RCLONE_CONFIG_SOURCE_PASS": "{{ conn.sftp_source_conn.password }}",
            "RCLONE_CONFIG_TARGET_TYPE": "sftp",
            "RCLONE_CONFIG_TARGET_HOST": "{{ conn.sftp_target_conn.host }}",
            "RCLONE_CONFIG_TARGET_USER": "{{ conn.sftp_target_conn.login }}",
            "RCLONE_CONFIG_TARGET_PASS": "{{ conn.sftp_target_conn.password }}",
        },
        conn_timeout=None,
        cmd_timeout=None,
        skip_on_exit_code=None,
    )


dag = sync_rclone()
