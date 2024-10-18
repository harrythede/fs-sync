import pendulum
from airflow.decorators import dag

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
    pass


dag = sync_rclone()
