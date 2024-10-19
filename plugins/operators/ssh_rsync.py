import re
from typing import Union

from airflow.providers.ssh.operators.ssh import SSHOperator


class SSHRsyncOperator(SSHOperator):

    def __init__(
            self,
            *,
            rsync_flags: Union[str, None] = None,
            rsync_filters: Union[list, None] = None,
            rsync_key_path: Union[str, None] = None,
            rsync_ssh_pass: Union[str, None] = None,
            rsync_source_path: Union[str, None] = None,
            rsync_target_host: Union[str, None] = None,
            rsync_target_user: Union[str, None] = None,
            rsync_target_path: Union[str, None] = None,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self.rsync_flags = rsync_flags or "-azP"
        self.rsync_filters = rsync_filters or []
        self.rsync_key_path = rsync_key_path
        self.rsync_ssh_pass = rsync_ssh_pass
        self.rsync_source_path = rsync_source_path or "/"
        self.rsync_target_path = rsync_target_path or "/"
        self.rsync_target_host = rsync_target_host
        self.rsync_target_user = rsync_target_user

        self.command = self._build_command()

    def _build_command(self):
        rsync_executable = "rsync"
        ssh_opts = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
        sshpass_opts = ""

        if self.rsync_key_path is not None:
            ssh_opts += f" -i {self.rsync_key_path}"
        elif self.rsync_ssh_pass is not None:
            sshpass_opts = f"sshpass -p {self.rsync_ssh_pass}"

        rsync_command = rsync_executable + f" -e '{ssh_opts}'"
        rsync_flags = self.rsync_flags
        rsync_filters = " ".join(map(lambda f: f"--filter={f}", self.rsync_filters))

        rsync_source = self.rsync_source_path
        if self.rsync_target_host is not None and self.rsync_target_user is not None:
            rsync_target = f"{self.rsync_target_user}@{self.rsync_target_host}:{self.rsync_target_path}"
        else:
            rsync_target = self.rsync_target_path

        command = " ".join((sshpass_opts,
                            rsync_command,
                            rsync_flags,
                            rsync_filters,
                            rsync_source,
                            rsync_target)).strip()
        command = re.sub(r"\s+", " ", command)

        self.log.info(command)
        return command
