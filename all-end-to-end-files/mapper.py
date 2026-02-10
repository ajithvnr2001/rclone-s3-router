#!/usr/bin/env python3
"""
PYTHON MASTER MAPPER (Auto-Discovery + Large File Detection)
Auto-discovers all subfolders, scans each for files with sizes,
separates normal files (≤ threshold) and large files (> threshold),
and uploads both lists + folder index to Wasabi S3.
"""

import subprocess
import boto3
import json

# ============ CONFIGURATION ============
SOURCE = "onedrive:Work Files"      # rclone remote:path to scan
S3_BUCKET = "workfiles123"
S3_PREFIX = "work_files_zips/"

AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
S3_ENDPOINT = "https://s3.ap-northeast-1.wasabisys.com"

LARGE_FILE_THRESHOLD_GB = 20  # Files larger than this go to the large files list
# =======================================

FOLDER_INDEX_KEY = f"{S3_PREFIX}_index/folder_list.txt"
LARGE_FILE_THRESHOLD_BYTES = LARGE_FILE_THRESHOLD_GB * 1024 * 1024 * 1024


def get_s3_client():
    return boto3.client(
        service_name='s3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url=S3_ENDPOINT
    )


def discover_folders():
    """Auto-discover all top-level subfolders in the source path."""
    print(f"🔍 Discovering folders in: {SOURCE}")
    cmd = [
        'rclone', 'lsf', SOURCE,
        '--dirs-only',
        '--config=/content/rclone.conf'
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        folders = [line.strip().rstrip('/') for line in result.stdout.splitlines() if line.strip()]
        print(f"   📁 Found {len(folders)} folders:")
        for f in folders:
            print(f"      • {f}")
        return folders
    except subprocess.CalledProcessError as e:
        print(f"   ❌ Error discovering folders: {e.stderr}")
        return []


def save_folder_index(folders):
    """Save the master folder list to S3."""
    s3 = get_s3_client()
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=FOLDER_INDEX_KEY,
        Body="\n".join(folders).encode('utf-8')
    )
    print(f"\n📋 Saved folder index to S3: {FOLDER_INDEX_KEY}")


def check_list_exists(s3, map_key):
    """Check if a file list already exists on S3."""
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=map_key)
        return True
    except:
        return False


def scan_folder_with_sizes(folder):
    """
    Scan a folder using rclone lsjson to get file paths AND sizes.
    Returns (normal_files, large_files) where each is a list of dicts.
    normal_files: list of relative paths (strings) for files ≤ threshold
    large_files: list of dicts {path, size} for files > threshold
    """
    folder_path = f"{SOURCE}/{folder}"

    cmd = [
        'rclone', 'lsjson', folder_path,
        '-R', '--files-only', '--no-mimetype', '--no-modtime',
        '--config=/content/rclone.conf'
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        entries = json.loads(result.stdout)

        normal_files = []
        large_files = []

        for entry in entries:
            path = entry.get('Path', '')
            size = entry.get('Size', 0)

            if size > LARGE_FILE_THRESHOLD_BYTES:
                large_files.append({
                    'path': path,
                    'size': size,
                    'size_gb': round(size / (1024 * 1024 * 1024), 2)
                })
            else:
                normal_files.append(path)

        return normal_files, large_files

    except subprocess.CalledProcessError as e:
        print(f" ❌ Error: {e.stderr[:80]}")
        return [], []


def run_mapper(force_rescan=False):
    s3 = get_s3_client()
    print("🚀 PYTHON MASTER MAPPER (Auto-Discovery + Large File Detection)")
    print("=" * 60)
    print(f"   Large file threshold: {LARGE_FILE_THRESHOLD_GB} GB")
    if force_rescan:
        print("   ⚠️  FORCE RESCAN enabled — will re-scan all folders")

    # 1. Auto-discover folders
    folders = discover_folders()
    if not folders:
        print("❌ No folders found. Exiting.")
        return

    # 2. Save folder index to S3
    save_folder_index(folders)

    # 3. Resume check
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

    # 4. Scan each folder with size detection
    print(f"\n📂 Scanning {len(folders)} folders (with size detection)...\n")

    total_normal = 0
    total_large = 0

    for folder in folders:
        clean_name = folder.replace(" ", "_")
        map_key = f"{S3_PREFIX}{clean_name}_List.txt"
        large_key = f"{S3_PREFIX}{clean_name}_LargeFiles.json"

        print(f"   📂 {folder} ...", end="", flush=True)

        normal_files, large_files = scan_folder_with_sizes(folder)

        # Upload normal files list
        if normal_files:
            file_list_data = "\n".join(normal_files)
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=map_key,
                Body=file_list_data.encode('utf-8')
            )
        else:
            # Upload empty list so resume doesn't re-scan
            s3.put_object(Bucket=S3_BUCKET, Key=map_key, Body=b"")

        # Upload large files list (JSON with paths and sizes)
        if large_files:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=large_key,
                Body=json.dumps(large_files, indent=2).encode('utf-8'),
                ContentType='application/json'
            )

        total_normal += len(normal_files)
        total_large += len(large_files)

        large_info = f", ⚡{len(large_files)} large" if large_files else ""
        print(f" ✅ {len(normal_files)} files{large_info}")

        # Print large file details
        if large_files:
            for lf in large_files:
                print(f"      🔴 LARGE: {lf['path']} ({lf['size_gb']} GB)")

    print(f"\n🎉 MAPPING COMPLETE!")
    print(f"   Normal files: {total_normal}")
    print(f"   Large files:  {total_large} (will be transferred directly)")


if __name__ == "__main__":
    run_mapper()