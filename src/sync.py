import logging
import json
import sys
import os
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
            logging.info(">>> Load old SFTP metadata to old_sftp_meta.")
        return old_sftp_meta
    except FileNotFoundError:
        logging.error(f">>> Old metadata file not found at {path}. Returning empty metadata.")
        return {}
    except json.JSONDecodeError:
        logging.error(f">>> Metadata file at {path} is empty or contains invalid JSON. Returning empty metadata.")
        return {}


def compare_sftp_meta(new_sftp_meta, old_sftp_meta):
    """
    Compare meta of files in sftp and s3 and returns list of files to upload to s3
    """
    files_to_upload = []

    for path, new_attrs in new_sftp_meta.items():
        if (path not in old_sftp_meta or 
            new_attrs["size"] != old_sftp_meta[path]["size"] or
            new_attrs["last_mod"] > old_sftp_meta[path]["last_mod"]):
            files_to_upload.append(path)
    
    logging.info(f">>> Files to upload: {files_to_upload}")
    return files_to_upload


def sftp_to_s3(s3, sftp, files_to_upload, local_dwnld_dir):
    """
    Load files from sftp to s3 frougth local dir
    """
    if not files_to_upload:
        logging.info(">>> Nothing to upload, files_to_upload list is empty.")
        return 

    if not os.path.exists(local_dwnld_dir):
        os.makedirs(local_dwnld_dir)
        logging.info(f">>> Created local directory: {local_dwnld_dir}")

    for path in files_to_upload:
        local_file_path = os.path.join(local_dwnld_dir, os.path.basename(path))
        logging.info(">>>>>>>>>>>")
        try:
            sftp.sftp_to_local(path, local_file_path)
        except:
            continue

        s3_path = path.replace('upload/', 'history/', 1)
        try:
            s3.local_to_s3(local_file_path, s3_path)
        except:
            continue

        try:
            os.remove(local_file_path)
            logging.info(f">>> Deleted local file {local_file_path} after upload to S3")
        except Exception as e:
            logging.error(f">>> Failed to delete local file {local_file_path}: {e}")


def sftp_to_s3_direct(s3, sftp, files_to_upload):
    """
    Directly upload files from SFTP to S3
    """
    if not files_to_upload:
        logging.info(">>> Nothing to upload, files_to_upload list is empty.")
        return 

    for path in files_to_upload:
        try:
            with sftp.sftp.open(path, "rb") as sftp_file:
                sftp_file.prefetch()
                logging.info(">>>>>>>>>>>")
                logging.info(f"{sftp_file}")

                s3_path = path.replace('upload/', 'history/', 1)

                s3.s3.put_object(
                    Bucket=s3.bucket,
                    Key=s3_path,
                    Body=sftp_file
                )
                logging.info(f">>> Uploaded {path} to S3 as {s3_path}")
        
        except Exception as e:
            logging.error(f">>> Failed to upload {path} to S3: {e}")
            continue


if __name__ == "__main__":
    logging.info(">>>    STARTING SYNC PROCCESS    <<<")
    #get old sftp meta
    old_sftp_meta = get_old_sftp_meta(PATH_TO_OLD_META)
    if not old_sftp_meta:
        logging.error("Old metadata is missing or empty. Stopping execution.")
        sys.exit(1)

    #get sftp connection
    sftp_creds = get_sftp_creds()
    sftp = SftpHandler(
        host=sftp_creds["sftp_host"],
        port=sftp_creds["sftp_port"],
        user=sftp_creds["sftp_user"],
        password=sftp_creds["sftp_pass"],
        path=sftp_creds["sftp_folder_name"]
    )

    #get new meta from sftp
    new_sftp_meta = sftp.get_meta()
    
    #get files to upload to s3
    files_to_upload = compare_sftp_meta(new_sftp_meta, old_sftp_meta)
    logging.info(f">>> Preparing to upload {len(files_to_upload)} files to S3.")

    if not files_to_upload:
        logging.info(">> No files to upload to s3.")
        sftp.close_conn()
        logging.info(">>> Exiting...")
        sys.exit(0)
    
    #get s3 connection
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

    #load data from sftp to s3
    sftp_to_s3_direct(s3, sftp, files_to_upload)
    logging.info(">>> Finish loading files from sftp to s3")
    
    sftp.close_conn()
    
    #remember new meta
    with open(PATH_TO_OLD_META, "w") as f:
        json.dump(new_sftp_meta, f)
    logging.info(">>> Update sftp meta in local.")

