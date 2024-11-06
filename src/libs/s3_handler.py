import logging
import os
import boto3
import boto3.session
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

class S3Handler:
    def __init__(self, bucket, folder, service_name, endpoint_url):
        self.service_name=service_name
        self.endpoint_url=endpoint_url
        self.bucket = bucket
        self.folder = folder
        self.s3 = self.get_conn()

    def get_conn(self):
        session = boto3.session.Session()
        try:
            self.s3 = session.client(
                service_name=self.service_name,
                endpoint_url=self.endpoint_url
            )
            response = self.s3.list_buckets()
            logging.info(f"Connection successful to {self.endpoint_url}, available buckets: {response['Buckets']}")
        except NoCredentialsError:
            logging.error("No credentials found. Ensure your S3 credentials are configured in ~/.aws/credentials.")
            raise
        except PartialCredentialsError:
            logging.error("Incomplete credentials found. Check your S3 configuration.")
            raise
        except Exception as e:
            logging.error(f"An error occurred while connecting: {e}")
            raise
        
        return self.s3
    

    def get_meta(self):
        s3_meta = {}
        try:
            paginator = self.s3.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket, Prefix=self.folder)

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        file_key = obj['Key']
                        if not file_key.endswith('/'):
                            last_mod_unix = obj['LastModified'].timestamp()
                            s3_meta[file_key] = {
                                "size": obj['Size'],
                                "last_mod": last_mod_unix
                            }
        except Exception as e:
            logging.error(f"Error while retrieving metadata from s3: {e}")

        return s3_meta
    

    def local_to_s3(self, files_to_upload, local_dwnld_dir):
        if not files_to_upload:
            logging.info("Nothing to upload, files_to_upload list is empty.")
            return 
        
        for path in files_to_upload:
            local_file_path = os.path.join(local_dwnld_dir, os.path.basename(path))

            try: 
                s3_path = path.replace('upload/', 'history/')
                logging.info(f"Starting upload from local: {local_file_path} to s3: {s3_path}")
                self.upload_file(local_file_path, self.bucket, s3_path)
                logging.info(f"Uploaded {path} to {s3_path}")
            except Exception as e:
                logging.error(f"Failed to upload {path}: {e}")
    
    def __getattr__(self, name):
        return getattr(self.s3, name)