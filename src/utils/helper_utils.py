import configparser
import shutil
import logging


def get_sftp_creds():
    config = configparser.ConfigParser()
    config.read("/sftp_to_s3/config/config.ini") 

    sftp_creds = {
        "sftp_host": config["SFTP"]["SFTP_HOST"],
        "sftp_port": int(config["SFTP"]["SFTP_PORT"]),
        "sftp_user": config["SFTP"]["SFTP_USER"],
        "sftp_pass": config["SFTP"]["SFTP_PASS"]
    }

    return sftp_creds


def clean_local_dwnld_dir(local_dwnld_dir):
    shutil.rmtree(local_dwnld_dir)
    logging.info(f"Removed local dir: {local_dwnld_dir}.")

