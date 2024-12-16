import pandas as pd
import os
import boto3

s3 = boto3.client('s3')

def download_files_from_s3(bucket, files, local_directory):
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    for file_key in files:
        s3_file = file_key[file_key.find("prefix=") + 7:-1] + "/run_options.yml"
        local_filename = file_key[file_key.find("run") + 4:-1] + ".yml"

        local_path = os.path.join(local_directory, local_filename)
        print(f"Downloading file {s3_file} to {local_path}...")
        try:
            s3.download_file(bucket, s3_file, local_path)
            print(f"Downloaded: {s3_file}")
        except Exception as e:
            print(f"Failed to download file {s3_file}: {e}")


bucket = "834599497928"
local_directory = "\\failed_files"

file = "\\output.xlsx"
sheet_name = "btap_data"
current_file_path = os.path.abspath(__file__)
current_directory = os.path.dirname(current_file_path)
file_path = current_directory + file
df = pd.read_excel(file_path, sheet_name=sheet_name)
filter_column = "status"
filter_value = "FAILED"
filtered_data = df[df[filter_column] == filter_value]

column_name = "datapoint_output_url"
failed_buckets = filtered_data[column_name]
failed_buckets_list = failed_buckets.tolist()

download_directory = os.path.join(current_directory + local_directory)

download_files_from_s3(bucket, failed_buckets_list, download_directory)
