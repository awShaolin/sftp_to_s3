#!/bin/bash

/usr/bin/flock -n /tmp/sftp_to_s3.lock /usr/local/bin/python /sftp_to_s3/src/full_sync.py

touch /sftp_to_s3/sync.log
touch /sftp_to_s3/full_sync.log

echo "*/1 * * * * /usr/bin/flock -n /tmp/sftp_to_s3.lock /usr/local/bin/python /sftp_to_s3/src/sync.py >> /sftp_to_s3/sync.log 2>&1" > /etc/cron.d/sync_cron
echo "0 */2 * * * /usr/bin/flock -n /tmp/sftp_to_s3.lock /usr/local/bin/python /sftp_to_s3/src/full_sync.py >> /sftp_to_s3/full_sync.log 2>&1" >> /etc/cron.d/sync_cron

chmod 0644 /etc/cron.d/sync_cron
crontab /etc/cron.d/sync_cron
service cron start

tail -f /sftp_to_s3/sync.log /sftp_to_s3/full_sync.log
