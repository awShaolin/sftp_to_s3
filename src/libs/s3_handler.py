import logging
import os
import boto3
import boto3.session
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

class S3Handler:
    def __init__(self, bucket, folder, service_name, endpoint_url, s3_access_key_id, s3_secret_access_key, s3_region):
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key
        self.s3_region = s3_region
        self.service_name=service_name
        self.endpoint_url=endpoint_url
        self.bucket = bucket
        self.folder = folder
        self.s3 = self.get_conn()

    def get_conn(self):
        """
        Get connection to s3
        """
        session = boto3.session.Session(
                aws_access_key_id=self.s3_access_key_id,
                aws_secret_access_key=self.s3_secret_access_key,
                region_name=self.s3_region
            )
        try:
            self.s3 = session.client(
                service_name=self.service_name,
                endpoint_url=self.endpoint_url
            )
            response = self.s3.list_buckets()
            logging.info(f">>> Connection successful to {self.endpoint_url}, available buckets: {response['Buckets']}")
        except NoCredentialsError:
            logging.error(">>> No credentials found. Ensure your S3 credentials are configured in ~/.aws/credentials or they defined in env.")
            raise
        except PartialCredentialsError:
            logging.error(">>> Incomplete credentials found. Check your S3 configuration.")
            raise
        except Exception as e:
            logging.error(f">>> An error occurred while connecting: {e}")
            raise
        
        return self.s3
    

    def get_meta(self):
        """
        Get s3 files meta
        """
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
            logging.error(f">>> Error while retrieving metadata from s3: {e}")

        return s3_meta
    

    def local_to_s3(self, local_file_path, s3_path):
        """
        Load data from local dir to s3
        """
        try: 
            logging.info(f">>> (TO S3) Starting to upload {local_file_path} to S3 ...")
            self.s3.upload_file(local_file_path, self.bucket, s3_path)
            logging.info(f">>> (TO S3) Uploaded {local_file_path} to S3 as {s3_path}")
        except Exception as e:
            logging.error(f">>> (TO S3) Failed to upload {local_file_path} to S3: {e}")
    
    def __getattr__(self, name):
        return getattr(self.s3, name)