import logging 
import json
import sys
from libs.s3_handler import S3Handler
from libs.sftp_handler import SftpHandler
from utils.helper_utils import get_sftp_creds, clean_local_dwnld_dir

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#local
PATH_TO_OLD_META = "/sftp_to_s3/tmp/sftp_meta.json"
LOCAL_DWNLD_DIR = "/sftp_to_s3/tmp/local_data"
#sftp
# SFTP_FOLDER_NAME = "upload"
SFTP_FOLDER_NAME = "upload/axapta/client_registration/2024"
#s3
S3_BUCKET_NAME = "mtest"
S3_FOLDER_NAME = "history/"
S3_SERVICE = 's3'
S3_ENDPOINT = 'https://storage.yandexcloud.net'

def compare_sftp_s3_meta(sftp_meta, s3_meta):
    files_to_upload = []

    for sftp_path, sftp_attrs in sftp_meta.items():
        s3_path = sftp_path.replace('upload/', 'history/')
        if s3_path not in s3_meta:
            files_to_upload.append(sftp_path)
        else:
            s3_attrs = s3_meta[s3_path]
            if (sftp_attrs['size'] != s3_attrs['size'] or 
                sftp_attrs['last_mod'] > int(s3_attrs['last_mod'])):
                files_to_upload.append(sftp_path)
    return files_to_upload

if __name__ == "__main__":
    logging.info("STARTING FULL SYNC PROCCESS")
    #get sftp meta
    sftp_creds = get_sftp_creds()

    sftp = SftpHandler(
        host=sftp_creds["sftp_host"],
        port=sftp_creds["sftp_port"],
        user=sftp_creds["sftp_user"],
        password=sftp_creds["sftp_pass"],
        path=SFTP_FOLDER_NAME
    )

    sftp_meta = sftp.get_meta()
    logging.info(f"sftp_meta: {sftp_meta}")

    #get s3 meta
    s3 = S3Handler(
        bucket=S3_BUCKET_NAME,
        folder=S3_FOLDER_NAME,
        service_name=S3_SERVICE,
        endpoint_url=S3_ENDPOINT 
    )
    s3_meta = s3.get_meta()
    logging.info(f"s3_meta: {s3_meta}")

    #get files to upload to s3
    files_to_upload = compare_sftp_s3_meta(sftp_meta, s3_meta)
    logging.info(f"Preparing to upload {len(files_to_upload)} files to s3.")

    if not files_to_upload:
        logging.info("No files to upload to s3.")

        with open(PATH_TO_OLD_META, "w") as f:
            json.dump(sftp_meta, f)
        logging.info("Update sftp meta in local.")

        sftp.close()
        logging.info("Close connection to sftp")
        logging.info("Exiting...")
        sys.exit(0)

    #load files from sftp to local
    sftp.sftp_to_local(files_to_upload, LOCAL_DWNLD_DIR)
    sftp.close()

    #load files from local to s3
    s3.local_to_s3(files_to_upload, LOCAL_DWNLD_DIR)

    clean_local_dwnld_dir(LOCAL_DWNLD_DIR)

    #remember sftp meta
    with open(PATH_TO_OLD_META, "w") as f:
        json.dump(sftp_meta, f)
    logging.info("Update sftp meta in local.")