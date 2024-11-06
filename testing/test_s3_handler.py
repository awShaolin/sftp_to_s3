import logging
from src.libs.s3_handler import S3Handler

S3_BUCKET_NAME = "mtest"
S3_FOLDER_NAME = "history/"
S3_SERVICE = 's3'
S3_ENDPOINT = 'https://storage.yandexcloud.net'

LOCAL_DWNLD_DIR = "/Users/aero/Documents/HOFF/testing_stuff/sftp_to_s3/tmp_files"

logging.basicConfig(level=logging.INFO)

s3 = S3Handler(
    bucket=S3_BUCKET_NAME,
    folder=S3_FOLDER_NAME,
    service_name=S3_SERVICE,
    endpoint_url=S3_ENDPOINT
)

meta = s3.get_meta()

print(meta)

files_to_upload  = ['upload/axapta/client_registration/2024/client_registration_test51_utf8.csv', 
                                     'upload/axapta/purchase/2024/purchase_052024.csv']

s3.local_to_s3(files_to_upload, LOCAL_DWNLD_DIR)