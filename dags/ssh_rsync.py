import pendulum
from airflow.decorators import dag

from common.constant import DEFAULT_ARGS
from operators.ssh_rsync import SSHRsyncOperator


@dag(
    dag_id="fs_sync_ssh_rsync",
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["file-system", "sync", "ssh"]
)
def ssh_rsync():
    SSHRsyncOperator(
        task_id="ssh_rsync",
        ssh_conn_id="sftp_source_conn",
        rsync_ssh_pass="{{ conn.sftp_target_conn.password }}",
        rsync_source_path="{{ var.value.fs_sync_path }}/",
        rsync_target_host="{{ conn.sftp_target_conn.host }}",
        rsync_target_user="{{ conn.sftp_target_conn.login }}",
        rsync_target_path="{{ var.value.fs_sync_path }}",
        conn_timeout=None,
        cmd_timeout=None,
        skip_on_exit_code=None,
    )

dag = ssh_rsync()
