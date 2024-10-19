import re
from typing import Union

from airflow.providers.ssh.operators.ssh import SSHOperator


class SSHRCloneOperator(SSHOperator):
    def __init__(
            self,
            *,
            source_path: str,
            target_path: str,
            source_name: Union[str, None] = None,
            target_name: Union[str, None] = None,
            source_config: Union[dict, None] = None,
            target_config: Union[dict, None] = None,
            secret_source_fields: Union[list, None] = None,
            secret_target_fields: Union[list, None] = None,
            rclone_flags: Union[list, None] = None,
            extra_env: Union[dict, None] = None,
            **kwargs
    ) -> None:
        super().__init__(**kwargs)

        self.rclone_flags = rclone_flags or ["--metadata", "--progress"]

        self.source_name = source_name.lower()
        self.source_config = source_config
        self.source_path = source_path
        self.secret_source_fields = secret_source_fields or []

        self.target_name = target_name.lower()
        self.target_config = target_config
        self.target_path = target_path
        self.secret_target_fields = secret_target_fields or []

        self.extra_env = extra_env

        self._build_command_and_environment()

    def _build_command_and_environment(self):
        rclone_executable = "rclone"
        rclone_command = "copy"
        rclone_flags = " ".join(self.rclone_flags)

        rclone_source = ((self.source_name + ":") if self.source_name else "") + self.source_path
        rclone_target = ((self.target_name + ":") if self.target_name else "") + self.target_path

        rclone_env = {}
        if self.source_name and self.source_config:
            for conf, value in self.source_config.items():
                rclone_env[f"RCLONE_CONFIG_{self.source_name.upper()}_{conf.upper()}"] = value
        if self.target_name and self.target_config:
            for conf, value in self.target_config.items():
                rclone_env[f"RCLONE_CONFIG_{self.target_name.upper()}_{conf.upper()}"] = value
        if self.extra_env:
            for conf, value in self.extra_env.items():
                rclone_env[conf] = value

        rclone_obscure = []
        if self.source_name:
            for secret_field in self.secret_source_fields:
                env_name = f"RCLONE_CONFIG_{self.source_name.upper()}_{secret_field.upper()}"
                rclone_obscure.append(f"{env_name}=$(rclone obscure ${{{env_name}}})")
        if self.target_name:
            for secret_field in self.secret_target_fields:
                env_name = f"RCLONE_CONFIG_{self.target_name.upper()}_{secret_field.upper()}"
                rclone_obscure.append(f"{env_name}=$(rclone obscure ${{{env_name}}})")

        rclone_obscure_ = " ".join(rclone_obscure)

        command = " ".join((rclone_obscure_,
                            rclone_executable,
                            rclone_command,
                            rclone_source,
                            rclone_target,
                            rclone_flags,)).strip()
        command = re.sub(r"\s+", " ", command)

        self.log.info(command)
        self.command = command

        self.log.info(rclone_env)
        self.environment = rclone_env
