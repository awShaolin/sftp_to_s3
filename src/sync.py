import logging
import json
import sys
from libs.sftp_handler import SftpHandler
from libs.s3_handler import S3Handler
from utils.helper_utils import get_sftp_creds, clean_local_dwnld_dir

logging.basicConfig(level=logging.INFO)

#local
PATH_TO_OLD_META = "/Users/aero/Documents/HOFF/testing_stuff/sftp_to_s3/test.json"
LOCAL_DWNLD_DIR = "/Users/aero/Documents/HOFF/testing_stuff/sftp_to_s3/tmp_files"
#sftp
SFTP_FOLDER_NAME = "upload"
#s3
S3_BUCKET_NAME = "mtest"
S3_FOLDER_NAME = "history/"
S3_SERVICE = 's3'
S3_ENDPOINT = 'https://storage.yandexcloud.net'


def get_old_sftp_meta(path):
    try:
        with open(path, "r") as f:
            old_sftp_meta = json.load(f)
            logging.info("Load old sftp metadata to old_sftp_meta.")
        return old_sftp_meta
    except FileNotFoundError:
        logging.error(f"Old metadata file not found at {path}.")
        return {}


def compare_sftp_meta(new_sftp_meta, old_sftp_meta):
    files_to_upload = []

    for path, new_attrs in new_sftp_meta.items():
        if (path not in old_sftp_meta or 
            new_attrs["size"] != old_sftp_meta[path]["size"] or
            new_attrs["last_mod"] > old_sftp_meta[path]["last_mod"]):
            files_to_upload.append(path)
    
    logging.info(f"Files to upload: {files_to_upload}")
    return files_to_upload


if __name__ == "__main__":
    #get old sftp meta
    old_sftp_meta = get_old_sftp_meta(PATH_TO_OLD_META)

    if not old_sftp_meta:
        logging.error("Old metadata is missing or empty. Stopping execution.")
        sys.exit(1)

    #get new sftp meta
    sftp_creds = get_sftp_creds()

    sftp = SftpHandler(
        host=sftp_creds["sftp_host"],
        port=sftp_creds["sftp_port"],
        user=sftp_creds["sftp_user"],
        password=sftp_creds["sftp_pass"],
        path=SFTP_FOLDER_NAME
    )

    new_sftp_meta = sftp.get_meta()
    
    #get files to upload to s3
    files_to_upload = compare_sftp_meta(new_sftp_meta, old_sftp_meta)
    logging.info(f"Preparing to upload {len(files_to_upload)} files to S3.")

    if not files_to_upload:
        logging.info("No files to upload to s3.")
        sftp.close()
        logging.info("Close connection to sftp")
        logging.info("Exiting...")
        sys.exit(0)

    #load files from sftp to local
    sftp.sftp_to_local(files_to_upload, LOCAL_DWNLD_DIR)
    sftp.close()
    
    #load files from local to s3
    s3 = S3Handler(
        bucket=S3_BUCKET_NAME,
        folder=S3_FOLDER_NAME,
        service_name=S3_SERVICE,
        endpoint_url=S3_ENDPOINT 
    )
    s3.local_to_s3(files_to_upload, LOCAL_DWNLD_DIR)

    clean_local_dwnld_dir(LOCAL_DWNLD_DIR)

    #remember new meta
    with open(PATH_TO_OLD_META, "w") as f:
        json.dump(new_sftp_meta, f)
    logging.info("Update sftp meta in local.")

