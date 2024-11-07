import configparser
import shutil
import logging
import os


def get_sftp_creds():
    sftp_creds = {
        "sftp_host": os.getenv("SFTP_HOST"),
        "sftp_port": int(os.getenv("SFTP_PORT", 22)),  # Default to 22 if not set
        "sftp_user": os.getenv("SFTP_USER"),
        "sftp_pass": os.getenv("SFTP_PASS")
    }
    missing_creds = [key for key, value in sftp_creds.items() if value is None]
    if missing_creds:
        raise ValueError(f"Missing required SFTP environment variables: {', '.join(missing_creds)}")
    
    return sftp_creds


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

