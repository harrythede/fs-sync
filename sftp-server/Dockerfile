FROM atmoz/sftp:alpine

COPY sshd_config /etc/ssh/sshd_config
COPY create-sftp-user /usr/local/bin/
RUN chmod +x /usr/local/bin/create-sftp-user

RUN apk add --update --no-cache openssh sshpass rsync rclone