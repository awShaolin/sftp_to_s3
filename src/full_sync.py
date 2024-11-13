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

def sftp_to_s3(files_to_upload, sftp, s3, bucket):
    failed_files = []  

    for sftp_path in files_to_upload:
        s3_path = sftp_path.replace('upload/', 'history/')
        logging.info(f"Uploading file {sftp_path} to S3 as {s3_path}")

        try:
            with io.BytesIO() as file_stream:
                try:
                    sftp.getfo(sftp_path, file_stream)
                except Exception as e:
                    logging.error(f"Failed to download file {sftp_path} from SFTP: {e}")
                    failed_files.append(sftp_path) 
                    continue  

                file_stream.seek(0)

                try:
                    s3.upload_fileobj(file_stream, bucket, s3_path)
                    logging.info(f"File {sftp_path} successfully uploaded to S3 as {s3_path}")
                except Exception as e:
                    logging.error(f"Failed to upload file {s3_path} to S3: {e}")
                    failed_files.append(sftp_path)
                    continue

        except Exception as e:
            logging.error(f"Unexpected error occurred while processing file {sftp_path}: {e}")
            failed_files.append(sftp_path)
            continue 

    if failed_files:
        logging.error(f"Failed to upload the following files: {', '.join(failed_files)}")
    else:
        logging.info("All files uploaded successfully.")


if __name__ == "__main__":
    logging.info("STARTING FULL SYNC PROCCESS")
    
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
    logging.info(f"sftp_meta: {sftp_meta}")

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

    #load data from sftp to s3
    sftp_to_s3(files_to_upload, sftp, s3, s3_creds["s3_bucket_name"])

    #close sftp connection
    sftp.close()
    logging.info("Close connection to sftp")

    #remember sftp meta
    with open(PATH_TO_OLD_META, "w") as f:
        json.dump(sftp_meta, f)
    logging.info("Update sftp meta in local.")