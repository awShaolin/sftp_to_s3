import os
import logging
from libs.s3_handler import S3Handler
from libs.sftp_handler import SftpHandler
from utils.helper_utils import get_sftp_creds

S3_BUCKET_NAME = "mtest"
S3_FOLDER_NAME = "history/"
S3_SERVICE = 's3'
S3_ENDPOINT = 'https://storage.yandexcloud.net'

SFTP_FOLDER_NAME = "upload"

LOCAL_DWNLD_DIR = "/Users/aero/Documents/HOFF/testing_stuff/sftp_to_s3/tmp_files"

logging.basicConfig(level=logging.INFO)


def get_files_to_upload(sftp_meta, s3_meta):
    files_to_upload = []

    for sftp_path, sftp_attrs in sftp_meta.items():
        s3_path = sftp_path.replace('upload/', 'history/')

        if s3_path not in s3_meta:
            files_to_upload.append(sftp_path)
        else:
            s3_attrs = s3_meta[s3_path]
            if (sftp_attrs['size'] != s3_attrs['size'] or sftp_attrs['last_mod'] > int(s3_attrs['last_mod'].timestamp())):
                files_to_upload.append(sftp_path)

    return files_to_upload


def load_to_s3(sftp, s3, files_to_upload):
    for sftp_path in files_to_upload:
        local_file_path = os.path.join(LOCAL_DWNLD_DIR, os.path.basename(sftp_path))

        try: 
            sftp.get(sftp_path, local_file_path)
            logging.info(f"Downloaded {sftp_path} to {local_file_path}")

            s3_path = sftp_path.replace('upload/', 'history/')

            s3.upload_file(local_file_path, S3_BUCKET_NAME, s3_path)
            logging.info(f"Uploaded {local_file_path} to s3://{S3_BUCKET_NAME}/{s3_path}")

        except Exception as e:
            logging.error(f"Failed to upload {sftp_path}: {e}")

        os.remove(local_file_path)
        logging.info(f"Removed local file {local_file_path}")

if __name__ == "__main__":
    sftp_creds = get_sftp_creds()

    sftp = SftpHandler(
        host=sftp_creds["sftp_host"],
        port=sftp_creds["sftp_port"],
        user=sftp_creds["sftp_user"],
        password=sftp_creds["sftp_pass"],
        path=SFTP_FOLDER_NAME
    )

    sftp_meta = sftp.get_meta(path='upload/axapta/purchase/2024')

    s3 = S3Handler(
        bucket=S3_BUCKET_NAME,
        folder=S3_FOLDER_NAME,
        service_name=S3_SERVICE,
        endpoint_url=S3_ENDPOINT
    )

    s3_meta = s3.get_meta()

    files_to_upload = get_files_to_upload(sftp_meta, s3_meta)

    load_to_s3(sftp, s3, files_to_upload)

    sftp.close()

