import re
import subprocess
from subprocess import CalledProcessError
from typing import Any, Literal, Union

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context


class RCloneOperator(BaseOperator):

    template_fields = ("source_path", "target_path", "command", "environment")

    _rclone_config_prefix = "RCLONE_CONFIG_"

    def __init__(
            self,
            *,
            source_name: str,
            source_conn_id: str,
            source_conn_type: Union[Literal["sftp"], Literal["s3"]],
            source_path: str,
            target_name: str,
            target_conn_id: str,
            target_conn_type: Union[Literal["sftp"], Literal["s3"]],
            target_path: str,
            rclone_executable: Union[str, None] = None,
            rclone_command: Union[Literal["copy"], Literal["sync"]] = "copy",
            source_extra_config: Union[dict, None] = None,
            target_extra_config: Union[dict, None] = None,
            options: Union[list, None] = None,
            **kwargs
    ):
        super().__init__(**kwargs)

        self.rclone_executable = rclone_executable if isinstance(rclone_executable, str) else "rclone"
        self.rclone_command = rclone_command

        self.source_name = source_name
        self.source_path = source_path
        self.target_name = target_name
        self.target_path = target_path

        source_env = self._parse_rclone_config(source_name, source_conn_id, source_conn_type, source_extra_config)
        target_env = self._parse_rclone_config(target_name, target_conn_id, target_conn_type, target_extra_config)
        self.environment = {**source_env, **target_env}

        self.options = options or ["--metadata", "--progress"]

        self.command = re.sub(r"\s+", " ", " ".join([
            " ".join(map(lambda entry: "=".join(entry), self.environment.items())),
            self.rclone_executable,
            self.rclone_command,
            self.source_name + ":" + self.source_path,
            self.target_name + ":" + self.target_path,
            " ".join(map(lambda o: o.strip(), self.options))
        ]).strip())

    def _parse_rclone_config(self, name: str, conn_id: str, conn_type: str, extra_config: Union[dict, None] = None) -> dict:
        rclone_config_remote_prefix = self._rclone_config_prefix + name.upper()
        config = {}

        if conn_type == "sftp":
            config = {
                rclone_config_remote_prefix + "_TYPE": "sftp",
                rclone_config_remote_prefix + "_HOST": f"{{{{ conn.{conn_id}.host }}}}",
                rclone_config_remote_prefix + "_USER": f"{{{{ conn.{conn_id}.login }}}}",
                rclone_config_remote_prefix + "_PASS": f"$(rclone obscure {{{{ conn.{conn_id}.password }}}})",
            }
        elif conn_type == "s3":
            config = {
                rclone_config_remote_prefix + "_TYPE": "s3",
                rclone_config_remote_prefix + "_ACCESS_KEY_ID": f"{{{{ conn.{conn_id}.login }}}}",
                rclone_config_remote_prefix + "_SECRET_ACCESS_KEY": f"$(rclone obscure {{{{ conn.{conn_id}.password }}}})",
            }

        if extra_config is not None:
            for conf, value in extra_config.items():
                config[rclone_config_remote_prefix + "_" + conf.upper()] = value

        return config

    def execute(self, context: Context) -> Any:
        self.log.info("Running command: %s", self.command)
        try:
            result = subprocess.run(self.command, shell=True, capture_output=True, text=True, check=True)
            self.log.info("Return code: %d", result.returncode)
            self.log.info("Stdout: %s", result.stdout)
            return result.returncode, result.stdout, result.stderr
        except CalledProcessError as e:
            self.log.error("Return code: %d", e.returncode)
            self.log.error("Stderr: %s", e.stderr)
            raise e
