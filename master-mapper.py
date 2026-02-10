#!/usr/bin/env python3
"""
PYTHON MASTER MAPPER (Auto-Discovery)
Auto-discovers all subfolders in OneDrive root, scans each,
and uploads file lists (.txt) + folder index to Wasabi S3.

The zipper and unzipper read from S3 to know which folders to process.
"""

import subprocess
import boto3

# ============ CONFIGURATION ============
SOURCE = "onedrive:Work Files"      # rclone remote:path to scan
S3_BUCKET = "workfiles123"
S3_PREFIX = "work_files_zips/"

AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
S3_ENDPOINT = "https://s3.ap-northeast-1.wasabisys.com"
# =======================================

# Key for the master folder index on S3
FOLDER_INDEX_KEY = f"{S3_PREFIX}_index/folder_list.txt"


def get_s3_client():
    return boto3.client(
        service_name='s3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url=S3_ENDPOINT
    )


def discover_folders():
    """Auto-discover all top-level subfolders in the OneDrive source path."""
    root_path = SOURCE
    print(f"🔍 Discovering folders in: {root_path}")

    cmd = [
        'rclone', 'lsf', root_path,
        '--dirs-only',
        '--config=/content/rclone.conf'
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        # rclone lsf returns folder names with trailing /, strip them
        folders = [line.strip().rstrip('/') for line in result.stdout.splitlines() if line.strip()]
        print(f"   📁 Found {len(folders)} folders:")
        for f in folders:
            print(f"      • {f}")
        return folders
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Error discovering folders: {e.stderr}")
        return []


def save_folder_index(folders):
    """Save the master folder list to S3 so zipper/unzipper can auto-discover."""
    s3 = get_s3_client()
    folder_data = "\n".join(folders)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=FOLDER_INDEX_KEY,
        Body=folder_data.encode('utf-8')
    )
    print(f"\n� Saved folder index to S3: {FOLDER_INDEX_KEY}")


def check_list_exists(s3, map_key):
    """Check if a folder's file list already exists on S3."""
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=map_key)
        return True
    except:
        return False


def run_mapper(force_rescan=False):
    s3 = get_s3_client()
    print("🚀 PYTHON MASTER MAPPER (Auto-Discovery + Resume)")
    print("=" * 50)
    if force_rescan:
        print("   ⚠️  FORCE RESCAN enabled — will re-scan all folders")

    # 1. Auto-discover folders
    folders = discover_folders()
    if not folders:
        print("❌ No folders found. Exiting.")
        return

    # 2. Save folder index to S3
    save_folder_index(folders)

    # 3. Check which folders already have file lists (resume)
    if not force_rescan:
        already_done = []
        remaining = []
        for folder in folders:
            clean_name = folder.replace(" ", "_")
            map_key = f"{S3_PREFIX}{clean_name}_List.txt"
            if check_list_exists(s3, map_key):
                already_done.append(folder)
            else:
                remaining.append(folder)

        if already_done:
            print(f"\n♻️  Resume: {len(already_done)} folders already mapped, skipping:")
            for f in already_done:
                print(f"      ⏭️  {f}")

        if not remaining:
            print("\n✅ All folders already mapped! Use force_rescan=True to re-scan.")
            return

        folders = remaining

    # 4. Scan each folder and upload file lists
    print(f"\n📂 Scanning {len(folders)} folders for files...\n")

    for folder in folders:
        clean_name = folder.replace(" ", "_")
        map_key = f"{S3_PREFIX}{clean_name}_List.txt"
        onedrive_path = f"{SOURCE}/{folder}"

        print(f"   📂 {folder} ...", end="", flush=True)

        cmd = [
            'rclone', 'lsf', onedrive_path,
            '-R', '--files-only',
            '--config=/content/rclone.conf'
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            file_list_data = result.stdout
            file_count = len([l for l in file_list_data.splitlines() if l.strip()])

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=map_key,
                Body=file_list_data.encode('utf-8')
            )
            print(f" ✅ {file_count} files → {map_key}")
        except subprocess.CalledProcessError as e:
            print(f" ❌ Error: {e.stderr[:80]}")

    print("\n🎉 ALL FOLDERS MAPPED SUCCESSFULLY!")


if __name__ == "__main__":
    run_mapper()
