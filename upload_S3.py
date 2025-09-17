import boto3
import os

s3 = boto3.client('s3')
bucket_name = "your-bucket-name"
local_folder = "Sleep dataset"

for root, dirs, files in os.walk(local_folder):
    for file in files:
        local_path = os.path.join(root, file)
        s3_key = os.path.relpath(local_path, local_folder)  # 保留相对路径
        print(f"Uploading {local_path} to s3://{bucket_name}/{s3_key}")
        s3.upload_file(local_path, bucket_name, s3_key)
