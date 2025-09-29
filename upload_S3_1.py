import boto3
import os

bucket_name = "ds-3021-project1"
repo_path = "/workspaces/sleepdataengineering"

files_to_upload = [
    "Africa.csv",
    "Oceania.csv",
    "South America.csv",
    "North America.csv",
    "Asia.csv",
    "Europe.csv"
]
s3 = boto3.client(
    "s3",
    region_name="eu-north-1",
    endpoint_url="https://s3.eu-north-1.amazonaws.com"
)

sts = boto3.client("sts", region_name="eu-north-1")
identity = sts.get_caller_identity()

for filename in files_to_upload:
    local_path = os.path.join(repo_path, filename)
    if os.path.exists(local_path):
        s3_key = f"RAW/{filename}"
        s3.upload_file(local_path, bucket_name, s3_key)
    else:
        print(f"{local_path}")
