FROM bitnami/airflow-worker:2.10.2-debian-12-r0

USER root
COPY rclone-v1.68.1-linux-amd64.deb .
RUN dpkg -i rclone-v1.68.1-linux-amd64.deb
RUN rclone version

USER 1001
