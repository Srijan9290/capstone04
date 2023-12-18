# Databricks notebook source
# container_name ="container1"
# account_name = "capstone004"
# storage_account_key="8I0hjFw2QdswQPvM2soP8A40nkErcuq/ThGbgYOf/bvjdg/3odiC9l6Y58FIAyYTOSbV3RC2BS2S+AStro7E3g=="
# mount_point = "/mnt/capstone_bronze"
# # Check if the storage is already mounted
# mounts = dbutils.fs.mounts()
# is_mounted = False
# for mount in mounts:
#   if mount.mountPoint == mount_point:
#     is_mounted = True

# # If not mounted, then mount it
# if not is_mounted:
#     dbutils.fs.mount(
#       source='wasbs://{0}@{1}.blob.core.windows.net'.format(container_name,account_name),
#       mount_point = mount_point,
#       extra_configs  = {"fs.azure.account.key.capstone004.blob.core.windows.net" : storage_account_key}
#     )
#     print(f"Storage mounted at {mount_point}")
# else:
#     print(f"Storage is already mounted at {mount_point}")

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import requests
from io import BytesIO

# COMMAND ----------

import subprocess
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

zip_file_url = "https://mentorskool-platform-uploads.s3.ap-south-1.amazonaws.com/documents/eee20a0c-15d3-405d-9bc6-e4e7fcebcc5a_83d04ac6-cb74-4a96-a06a-e0d5442aa126_Banking_data.zip"

local_zip_file_path = "/dbfs/mnt/capstone_bronze/dataset/Banking_data.zip"
local_unzip_dir = "/dbfs/mnt/capstone_bronze/dataset/unzipped"

subprocess.run(["wget", "-O", local_zip_file_path, zip_file_url], check=True)
subprocess.run(["unzip", "-q", local_zip_file_path, "-d", local_unzip_dir], check=True)

azure_connection_string = "DefaultEndpointsProtocol=https;AccountName=capstone004;AccountKey=8I0hjFw2QdswQPvM2soP8A40nkErcuq/ThGbgYOf/bvjdg/3odiC9l6Y58FIAyYTOSbV3RC2BS2S+AStro7E3g==;EndpointSuffix=core.windows.net"

container_name = "container1"
folder_name = "unzipped"

blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_client = blob_service_client.get_container_client(container_name)

local_files = os.listdir(local_unzip_dir)

for file_name in local_files:
    local_file_path = os.path.join(local_unzip_dir, file_name)
    blob_path = os.path.join(folder_name, file_name)
    blob_client = container_client.get_blob_client(blob_path)
    with open(local_file_path, "rb") as dataset:
        blob_client.upload_blob(dataset)

print("Uploaded all files to Azure Blob Storage in the 'unzipped' folder")

if len(local_files) == len(container_client.list_blobs(name_starts_with=folder_name)):
    print("All files have been uploaded. Stopping the execution.")
    os.remove(local_zip_file_path)
    for file_name in local_files:
        local_file_path = os.path.join(local_unzip_dir, file_name)
        os.remove(local_file_path)
    print("Cleaned up local files")
    exit()

print("Not all files have been uploaded. The script will continue.")


# COMMAND ----------

display(dbutils.fs.ls("/mnt/capstone_bronze/dataset/unzipped"))

# COMMAND ----------


