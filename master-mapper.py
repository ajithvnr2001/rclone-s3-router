#!/usr/bin/env python3
"""
PYTHON MASTER MAPPER
Scans OneDrive folders and uploads file lists (.txt) to Wasabi S3.
"""

import subprocess
import boto3
import io

# ============ CONFIGURATION ============
SUBFOLDERS = [
     "GHA Projects", "Pryor & Morrow Projects", "D",
    "Karlsberger Projects", "portfolio & resume", "Misc Reference Projects",
    "Pryor & Morrow Master Spec", "Specifications", "Revit Training",
    "Software Installation""My Companies"
]

ONEDRIVE_REMOTE = "onedrive:"
SOURCE_PATH = "Work Files"
S3_BUCKET = "workfiles123"
S3_PREFIX = "work_files_zips/"

AWS_ACCESS_KEY = "key"
AWS_SECRET_KEY = "ksyid"
S3_ENDPOINT = "https://s3.ap-northeast-1.wasabisys.com"
# =======================================

def get_s3_client():
    return boto3.client(
        service_name='s3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url=S3_ENDPOINT
    )

def run_mapper():
    s3 = get_s3_client()
    print("🚀 STARTING PYTHON MASTER MAPPING...")

    for folder in SUBFOLDERS:
        clean_name = folder.replace(" ", "_")
        map_key = f"{S3_PREFIX}{clean_name}_List.txt"
        onedrive_path = f"{ONEDRIVE_REMOTE}{SOURCE_PATH}/{folder}"

        print(f"📂 Scanning: {folder} ...", end="", flush=True)

        # Run rclone lsf to get the file list
        cmd = [
            'rclone', 'lsf', onedrive_path,
            '-R', '--files-only',
            '--max-size=100M',#remove this if size limit not needed
            '--config=/content/rclone.conf'
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            file_list_data = result.stdout

            # Upload the string data directly to S3 as a text file
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=map_key,
                Body=file_list_data.encode('utf-8')
            )
            print(f" ✅ Saved to S3: {map_key}")
        except subprocess.CalledProcessError as e:
            print(f" ❌ Error scanning {folder}: {e.stderr}")

    print("\n🎉 ALL FOLDERS MAPPED SUCCESSFULLY!")

if __name__ == "__main__":
    run_mapper()
