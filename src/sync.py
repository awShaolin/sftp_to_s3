import logging
import json
import sys
from libs.sftp_handler import SftpHandler
from libs.s3_handler import S3Handler
from utils.helper_utils import get_sftp_creds, get_s3_creds, clean_local_dwnld_dir

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#local
PATH_TO_OLD_META = "/sftp_to_s3/tmp/sftp_meta.json"
LOCAL_DWNLD_DIR = "/sftp_to_s3/tmp/local_data"


def get_old_sftp_meta(path):
    try:
        with open(path, "r") as f:
            old_sftp_meta = json.load(f)
            logging.info("Load old SFTP metadata to old_sftp_meta.")
        return old_sftp_meta
    except FileNotFoundError:
        logging.error(f"Old metadata file not found at {path}. Returning empty metadata.")
        return {}
    except json.JSONDecodeError:
        logging.error(f"Metadata file at {path} is empty or contains invalid JSON. Returning empty metadata.")
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
    logging.info("STARTING SYNC PROCCESS")
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
        path=sftp_creds["sftp_folder_name"]
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
    s3_creds = get_s3_creds()

    s3 = S3Handler(
        bucket=s3_creds["s3_bucket_name"],
        folder=s3_creds["s3_folder_name"],
        service_name=s3_creds["s3_service"],
        endpoint_url=s3_creds["s3_endpoint"], 
        s3_region=s3_creds["s3_region"],
        s3_access_key_id=s3_creds["s3_access_key_id"],
        s3_secret_access_key=s3_creds["s3_secret_access_key"]
    )
    s3.local_to_s3(files_to_upload, LOCAL_DWNLD_DIR)

    clean_local_dwnld_dir(LOCAL_DWNLD_DIR)

    #remember new meta
    with open(PATH_TO_OLD_META, "w") as f:
        json.dump(new_sftp_meta, f)
    logging.info("Update sftp meta in local.")

