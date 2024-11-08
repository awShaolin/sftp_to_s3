# Overview

This repository contains a service for transferring files from SFTP to S3 storage. It can be deployed using Docker locally or on a cloud-based virtual machine (VM). The service periodically syncs files, transferring any new or updated files from the SFTP server to the designated S3 bucket.

## Installation

### Build and Run the Docker Container:

Navigate to the project’s root directory: 
```bash
cd /sftp_to_s3
```

Build the Docker image:
```bash
docker build -t sftp_to_s3 .
```

Run the container, setting the necessary credentials as environment variables:

```bash
docker run -d --name sftp_to_s3_container \
  -e SFTP_HOST=sftphost.example \
  -e SFTP_PORT=22 \
  -e SFTP_USER=sftp_user \
  -e SFTP_PASS=sftp_pass \
  -e SFTP_FOLDER_NAME=folder \
  -e S3_ENDPOINT=endpoint \
  -e S3_SERVICE=s3 \
  -e S3_BUCKET_NAME=bucket_name \
  -e S3_FOLDER_NAME=folder/ \
  -e S3_REGION=region \
  -e S3_ACCESS_KEY_ID=some_key_id \
  -e S3_SECRET_ACCESS_KEY=some_key \
  sftp_to_s3
```

### Environment Variables
Define the following environment variables to configure the connection details:

 | Переменная | Описание |
 |---------|--------------------|
 | `SFTP_HOST` | Hostname of the SFTP server |
 | `SFTP_PORT` | Port number for the SFTP server connection (22 default) |
 | `SFTP_USER` | Username for accessing the SFTP server |
 | `SFTP_PASS` | Password for accessing the SFTP server |
 | `SFTP_FOLDER_NAME` | Path to the folder on the SFTP server to be synced |
 | `S3_ENDPOINT` | Endpoint URL for the S3-compatible storage |
 | `S3_SERVICE` | S3 service name (usually "s3") |
 | `S3_BUCKET_NAME` | Name of the target S3 bucket |
 | `S3_FOLDER_NAME` | Destination folder path in the S3 bucket |
 | `S3_REGION` | Region for the S3 storage |
 | `S3_ACCESS_KEY_ID` | Access key ID for S3 authentication |
 | `S3_SECRET_ACCESS_KEY` | Secret access key for S3 authentication |

 ## Prerequisites

- Docker ([download link](https://www.docker.com/products/docker-desktop/)) 
- Access to the SFTP server with read permissions
- Access to S3-compatible storage with necessary permissions

## Components
 
 | Компонент | Описание |
 |---------|--------------------|
 | `src/libs/s3_handler.py` | Handles S3 interactions, including uploading and metadata management |
 | `src/libs/sftp_handler.py` | Handles SFTP interactions, including file listing and downloading |
 | `src/utils/helper_utils.py` | Utility functions, such as retrieving credentials |
 | `src/full_sync.py` | Performs a full synchronization of files from SFTP to S3 |
 | `src/sync.py` | Executes periodic sync operations based on detected file changes|

 ## Full Sync and Sync Process Overview
 The full_sync.py and sync.py scripts are designed to transfer files from an SFTP server to an S3-compatible storage system. These scripts use cron jobs for automated scheduling, ensuring periodic updates and efficient file synchronization.

 #### Cron Schedules:

 - sync.py: Runs every minute, from the 1st to the 56th minute of each hour, avoiding the top of the hour.
 - full_sync.py: Runs every 2 hours on the hour, performing a comprehensive file synchronization.

 #### Flock Locking Mechanism
 The flock command ensures that only one instance of sync.py or full_sync.py runs at a time. This prevents potential conflicts by acquiring a lock file (`/tmp/sftp_to_s3.lock`), so if a script is already running, subsequent scheduled executions will not start until the current one finishes.