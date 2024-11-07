#!/bin/bash

# Load environment variables from .env and export to /etc/environment
if [ -f /sftp_to_s3/.env ]; then
    export $(grep -v '^#' /sftp_to_s3/.env | xargs -d '\n')
    grep -v '^#' /sftp_to_s3/.env >> /etc/environment
else
    echo "Warning: .env file not found in /sftp_to_s3"
fi

# Run the full_sync.py script immediately at startup with lock
/usr/bin/flock -n /tmp/sftp_to_s3.lock /usr/local/bin/python /sftp_to_s3/src/full_sync.py

# Create log files for cron jobs if they don't exist
touch /sftp_to_s3/sync.log
touch /sftp_to_s3/full_sync.log

# Set up cron jobs without additional sourcing
echo "*/1 * * * * /usr/bin/flock -n /tmp/sftp_to_s3.lock /usr/local/bin/python /sftp_to_s3/src/sync.py >> /sftp_to_s3/sync.log 2>&1" > /etc/cron.d/sync_cron
echo "0 */2 * * * /usr/bin/flock -n /tmp/sftp_to_s3.lock /usr/local/bin/python /sftp_to_s3/src/full_sync.py >> /sftp_to_s3/full_sync.log 2>&1" >> /etc/cron.d/sync_cron

# Set permissions and start cron
chmod 0644 /etc/cron.d/sync_cron
crontab /etc/cron.d/sync_cron
service cron start

# Tail the log files to keep the container running
tail -f /sftp_to_s3/sync.log /sftp_to_s3/full_sync.log