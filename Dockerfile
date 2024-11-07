FROM python:3.10-slim

WORKDIR /sftp_to_s3

RUN apt-get update && apt-get install -y cron

COPY requirements.txt /sftp_to_s3/requirements.txt
RUN pip install --no-cache-dir -r /sftp_to_s3/requirements.txt

COPY . /sftp_to_s3

RUN chmod +x /sftp_to_s3/entrypoint.sh

CMD ["/sftp_to_s3/entrypoint.sh"]