# sftp_to_s3

docker build -t sftp_to_s3 .

docker run -d --name sftp_to_s3_container \
  --env-file .env \
  -v ~/.aws:/root/.aws \
  sftp_to_s3