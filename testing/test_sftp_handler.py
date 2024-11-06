import configparser
import logging 
from src.libs.sftp_handler import SftpHandler

SFTP_FOLDER_NAME = "upload"
LOCAL_DWNLD_DIR = "/Users/aero/Documents/HOFF/testing_stuff/sftp_to_s3/tmp_files"


logging.basicConfig(level=logging.INFO)

config = configparser.ConfigParser()
config.read("config/config.ini")

sftp_creds = {
    "sftp_host": config["SFTP"]["SFTP_HOST"],
    "sftp_port": int(config["SFTP"]["SFTP_PORT"]),
    "sftp_user": config["SFTP"]["SFTP_USER"],
    "sftp_pass": config["SFTP"]["SFTP_PASS"]
}

sftp = SftpHandler(
    host=sftp_creds["sftp_host"],
    port=sftp_creds["sftp_port"],
    user=sftp_creds["sftp_user"],
    password=sftp_creds["sftp_pass"],
    path=SFTP_FOLDER_NAME
)

meta = sftp.get_meta(path='upload/axapta/purchase/2024')

print(meta)

files_to_upload  = ['upload/axapta/client_registration/2024/client_registration_test51_utf8.csv', 
                                     'upload/axapta/purchase/2024/purchase_052024.csv']

sftp.sftp_to_local(files_to_upload, LOCAL_DWNLD_DIR)

sftp.close()