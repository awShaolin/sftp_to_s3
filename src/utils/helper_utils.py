import configparser
import shutil
import logging
import os


def get_sftp_creds():
    sftp_creds = {
        "sftp_host": os.getenv("SFTP_HOST"),
        "sftp_port": int(os.getenv("SFTP_PORT", 22)),  # Default to 22 if not set
        "sftp_user": os.getenv("SFTP_USER"),
        "sftp_pass": os.getenv("SFTP_PASS"),
        "sftp_folder_name": os.getenv("SFTP_FOLDER_NAME")
    }
    missing_creds = [key for key, value in sftp_creds.items() if value is None]
    if missing_creds:
        raise ValueError(f"Missing required SFTP environment variables: {', '.join(missing_creds)}")
    
    return sftp_creds


def get_s3_creds():
    s3_creds = {
        "s3_endpoint": os.getenv("S3_ENDPOINT"),
        "s3_service": os.getenv("S3_SERVICE"),
        "s3_bucket_name": os.getenv("S3_BUCKET_NAME"),
        "s3_folder_name": os.getenv("S3_FOLDER_NAME"),
        "s3_region": os.getenv("S3_REGION"),
        "s3_access_key_id": os.getenv("S3_ACCESS_KEY_ID"),
        "s3_secret_access_key": os.getenv("S3_SECRET_ACCESS_KEY")
    }
    missing_creds = [key for key, value in s3_creds.items() if value is None]
    if missing_creds:
        raise ValueError(f"Missing required s3 environment variables: {', '.join(missing_creds)}")
    
    return s3_creds


# if you prefer to use config to access sftp creds use this method instead
# def get_sftp_creds():
#     config = configparser.ConfigParser()
#     config.read("config/config.ini")

#     sftp_creds = {
#         "sftp_host": config["SFTP"]["SFTP_HOST"],
#         "sftp_port": int(config["SFTP"]["SFTP_PORT"]),
#         "sftp_user": config["SFTP"]["SFTP_USER"],
#         "sftp_pass": config["SFTP"]["SFTP_PASS"]
#     }

#     return sftp_creds


def clean_local_dwnld_dir(local_dwnld_dir):
    shutil.rmtree(local_dwnld_dir)
    logging.info(f"Removed local dir: {local_dwnld_dir}.")

