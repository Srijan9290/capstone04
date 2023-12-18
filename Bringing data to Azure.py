# Databricks notebook source
# Databricks notebook source
account_name="capstone004"
container_name = "container1"
storage_acc_key = "8I0hjFw2QdswQPvM2soP8A40nkErcuq/ThGbgYOf/bvjdg/3odiC9l6Y58FIAyYTOSbV3RC2BS2S+AStro7E3g=="

#dbutils.fs.mount(
#source = "wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
#mount_point = "/mnt/batchdata/",
 # extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_acc_key}
  #)
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import requests
from io import BytesIO

import subprocess
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

zip_file_url = "https://mentorskool-platform-uploads.s3.ap-south-1.amazonaws.com/documents/eee20a0c-15d3-405d-9bc6-e4e7fcebcc5a_83d04ac6-cb74-4a96-a06a-e0d5442aa126_Banking_data.zip"


local_zip_file_path = "/dbfs/mnt/container1/data/data.zip"
local_unzip_dir = "/dbfs/mnt/container1/data/unzipped_data"
 
subprocess.run(["curl", "-O", local_zip_file_path, zip_file_url], check=True)
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

    with open(local_file_path, "rb") as data:

        blob_client.upload_blob(data)

 

print("Uploaded all files to Azure Blob Storage in the 'unzipped' folder")

 

os.remove(local_zip_file_path)

for file_name in local_files:

    local_file_path = os.path.join(local_unzip_dir, file_name)

    os.remove(local_file_path)

print("Cleaned up local files")

# COMMAND ----------

import subprocess
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import zipfile

account_name = "capstone004"
container_name = "container1"
storage_acc_key = "8I0hjFw2QdswQPvM2soP8A40nkErcuq/ThGbgYOf/bvjdg/3odiC9l6Y58FIAyYTOSbV3RC2BS2S+AStro7E3g=="
zip_file_url = "https://mentorskool-platform-uploads.s3.ap-south-1.amazonaws.com/documents/eee20a0c-15d3-405d-9bc6-e4e7fcebcc5a_83d04ac6-cb74-4a96-a06a-e0d5442aa126_Banking_data.zip"
local_zip_file_path = "/dbfs/mnt/container1/data/data.zip"
local_unzip_dir = "/dbfs/mnt/container1/data/unzipped_data"

# Download the file using curl
subprocess.run(["curl", "-o", local_zip_file_path, zip_file_url], check=True)

# Check available memory before extraction
memory_status = subprocess.run(["free", "-m"], capture_output=True, text=True)
print("Memory Status:")
print(memory_status.stdout)

# Use Python's zipfile module for extraction
with zipfile.ZipFile(local_zip_file_path, "r") as zip_ref:
    zip_ref.extractall(local_unzip_dir)

# Azure Storage connection details
azure_connection_string = "DefaultEndpointsProtocol=https;AccountName=capstone004;AccountKey=8I0hjFw2QdswQPvM2soP8A40nkErcuq/ThGbgYOf/bvjdg/3odiC9l6Y58FIAyYTOSbV3RC2BS2S+AStro7E3g==;EndpointSuffix=core.windows.net"
container_name = "container1"
folder_name = "unzipped"

# Azure Storage Blob client setup
blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Upload unzipped files to Azure Storage Blob
local_files = os.listdir(local_unzip_dir)
for file_name in local_files:
    local_file_path = os.path.join(local_unzip_dir, file_name)
    blob_path = os.path.join(folder_name, file_name)
    blob_client = container_client.get_blob_client(blob_path)
    with open(local_file_path, "rb") as data:
        blob_client.upload_blob(data)

# Clean up local files
os.remove(local_zip_file_path)
for file_name in local_files:
    local_file_path = os.path.join(local_unzip_dir, file_name)
    os.remove(local_file_path)

print("Uploaded all files to Azure Blob Storage in the 'unzipped' folder")
print("Cleaned up local files")


# COMMAND ----------


