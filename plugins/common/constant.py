from datetime import timedelta

import pendulum

DEFAULT_ARGS = {
    "owner": "Harry",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 10, 1),
    "email": ["harrytran001221@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
