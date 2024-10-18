import pendulum
from airflow.decorators import dag
from airflow.providers.ssh.operators.ssh import SSHOperator


@dag(
    dag_id="sftp_rsync",
    default_args={"owner": "harry"},
    schedule="0 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["file-system", "synchronization"]
)
def sftp_rsync():

    SSHOperator(
        task_id="rsync",
        ssh_conn_id="sftp_target_conn",
        command="sshpass -p {{ conn.sftp_source_conn.password }} " 
                "rsync -e 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null' -azP "
                "{{ conn.sftp_source_conn.login }}@{{ conn.sftp_source_conn.host }}:{{ var.value.sync_path }}/ {{ var.value.sync_path }}",
        conn_timeout=None,
        cmd_timeout=None,
        skip_on_exit_code=None,
    )


dag = sftp_rsync()
