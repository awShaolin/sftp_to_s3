import logging 
import json
import sys
import os
import io
from libs.s3_handler import S3Handler
from libs.sftp_handler import SftpHandler
from utils.helper_utils import get_sftp_creds, get_s3_creds, clean_local_dwnld_dir

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#local
PATH_TO_OLD_META = "/sftp_to_s3/tmp/sftp_meta.json"
LOCAL_DWNLD_DIR = "/sftp_to_s3/tmp/local_data"

def compare_sftp_s3_meta(sftp_meta, s3_meta):
    """
    Compare meta of files in sftp and s3 and returns list of files to upload to s3
    """
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
    logging.info(">>>    STARTING FULL SYNC PROCCESS   <<<")
    
    #get sftp meta
    sftp_creds = get_sftp_creds()
    sftp = SftpHandler(
        host=sftp_creds["sftp_host"],
        port=sftp_creds["sftp_port"],
        user=sftp_creds["sftp_user"],
        password=sftp_creds["sftp_pass"],
        path=sftp_creds["sftp_folder_name"]
    )
    sftp_meta = sftp.get_meta()
    logging.info(f">>> sftp_meta: {sftp_meta}")

    #get s3 meta
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
    s3_meta = s3.get_meta()
    logging.info(f">>> s3_meta: {s3_meta}")

    #get files to upload to s3
    files_to_upload = compare_sftp_s3_meta(sftp_meta, s3_meta)
    logging.info(f">>> Preparing to upload {len(files_to_upload)} files to s3.")

    if not files_to_upload:
        logging.info(">>> No files to upload to s3.")

        with open(PATH_TO_OLD_META, "w") as f:
            json.dump(sftp_meta, f)
        logging.info(">>> Update sftp meta in local.")

        sftp.close_conn()
        logging.info(">>> Exiting...")
        sys.exit(0)

    #load data from sftp to s3
    sftp_to_s3_direct(s3, sftp, files_to_upload)
    logging.info(">>> Finish loading files from sftp to s3")
    
    sftp.close_conn()
    
    clean_local_dwnld_dir(LOCAL_DWNLD_DIR)

    #remember sftp meta
    with open(PATH_TO_OLD_META, "w") as f:
        json.dump(sftp_meta, f)
    logging.info(">>> Update sftp meta in local.")