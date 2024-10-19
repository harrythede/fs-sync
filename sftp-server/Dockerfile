FROM atmoz/sftp:alpine

RUN apk add --update --no-cache openssh sshpass rsync rclone

COPY sshd_config /etc/ssh/sshd_config
COPY create-sftp-user /usr/local/bin/
RUN chmod +x /usr/local/bin/create-sftp-user