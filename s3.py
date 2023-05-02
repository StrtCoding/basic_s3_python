from boto3.session import Session
import boto3

import os

import settings

session = Session(aws_access_key_id=settings.AWS_ID, 
                aws_secret_access_key=settings.AWS_SECRET)


def download_file(bucket_name, bucket_path, dest_path):

    print(f"Downloading file...")
    
    s3 = session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.download_file(bucket_path, dest_path)

    print(f"Download File {bucket_path} at {dest_path}")


def get_files_objects(bucket_name, bucket_folder):

    s3_client = boto3.client("s3")

    all_objs = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{bucket_folder}", Delimiter="/")

    files_objs = []

    for obj in all_objs['Contents']:
        if obj['Key'] == bucket_folder:
            continue
        files_objs.append(obj)
    
    return files_objs


def download_all_files(bucket_name, bucket_folder):
    
    files_objs = get_files_objects(bucket_name, bucket_folder)

    for obj in files_objs:
        bucket_file_path = obj['Key']
        file_name = os.path.basename(bucket_file_path)
        dest_path = f"files/{file_name}"

        download_file(bucket_name, bucket_file_path, dest_path)


if __name__ == '__main__':
    bucket_name = settings.AWS_BUTCKET
    file_path = "files-folder/totals-watch-time.csv"
    out_path = "files/totals-watch-time.csv"
    
    download_all_files(bucket_name, "files-folder/")