#!/usr/bin/env python3
"""
PYTHON UNZIPPER & MERGER (Colab Compatible + S3 Resume)
Reverse of python_zipper.py — Downloads zips from S3, unzips, and merges them
back into the original folder structure on the target remote.

Features:
- Auto-dependency install (Rclone/Unzip)
- Downloads all zip parts from S3 (handles splits: Part1, Part1_Split1, etc.)
- Unzips and merges all parts into a single folder
- Uploads merged folder back to the target remote via rclone
- Smart Disk Management (cleans up after each part)
- S3 Progress Tracking (Saves progress JSON to S3 after every zip processed)
- Crash Resume (Reads progress from S3 on startup, skips completed work)
- Parallel processing with live status monitor
"""

import subprocess
import sys
import time
import boto3
import json
import math
import concurrent.futures
import multiprocessing
import os
import shutil
import stat
import random
import re

# ============ CONFIGURATION ============
# Subfolders that were zipped — must match your python_zipper.py config
SUBFOLDERS = [
    "Pryor & Morrow Projects", "My Companies"
]

# Target remote to upload the merged/unzipped files
# Change this to your destination remote (Google Drive, another OneDrive, etc.)
TARGET_REMOTE = "gdrive:"           # e.g. "gdrive:", "onedrive2:", etc.
TARGET_PATH = "Work Files"          # Destination base path on the remote

# S3 / Wasabi config — must match your python_zipper.py config
S3_BUCKET = "workfiles123"
S3_PREFIX = "work_files_zips/"

AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
S3_ENDPOINT = "https://s3.ap-northeast-1.wasabisys.com"

# Tuning
MAX_PARALLEL_WORKERS = 2    # Number of simultaneous folders to process
UPLOAD_THREADS = 6          # Rclone transfers per worker for upload
DISK_LIMIT_PERCENT = 80     # Trigger cleanup at this disk usage %
SKIP_UPLOAD = False         # Set True to only unzip locally without uploading
LOCAL_OUTPUT_DIR = "/content/merged_output"  # Local output when SKIP_UPLOAD=True
# =======================================

# ============ S3 PROGRESS TRACKING ============
PROGRESS_KEY = f"{S3_PREFIX}_progress/unzipper_progress.json"


def get_s3_client():
    return boto3.client(
        service_name='s3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url=S3_ENDPOINT
    )


def load_progress():
    """Load progress JSON from S3. Returns dict or empty dict on failure."""
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=PROGRESS_KEY)
        return json.loads(response['Body'].read().decode('utf-8'))
    except:
        return {}


def save_progress(progress):
    """Save progress JSON to S3."""
    s3 = get_s3_client()
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=PROGRESS_KEY,
            Body=json.dumps(progress, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
    except Exception as e:
        print(f"⚠️  Failed to save progress to S3: {e}")


def mark_zip_processed(folder_name, s3_key):
    """Mark a single zip as downloaded + unzipped + uploaded."""
    progress = load_progress()
    if folder_name not in progress:
        progress[folder_name] = {"processed_keys": [], "folder_complete": False}
    if s3_key not in progress[folder_name]["processed_keys"]:
        progress[folder_name]["processed_keys"].append(s3_key)
    save_progress(progress)


def mark_folder_complete(folder_name):
    """Mark an entire folder as fully completed."""
    progress = load_progress()
    if folder_name not in progress:
        progress[folder_name] = {"processed_keys": [], "folder_complete": False}
    progress[folder_name]["folder_complete"] = True
    save_progress(progress)


def is_folder_complete(folder_name):
    """Check if folder was fully processed in a previous run."""
    progress = load_progress()
    return progress.get(folder_name, {}).get("folder_complete", False)


def get_processed_keys(folder_name):
    """Get the set of S3 keys already processed for a folder."""
    progress = load_progress()
    return set(progress.get(folder_name, {}).get("processed_keys", []))
# ================================================


def get_folder_size_mb(path):
    """Calculates folder size safely"""
    total_size = 0
    try:
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    except:
        pass
    return total_size / (1024 * 1024)


def check_disk_usage():
    """Returns True if disk usage is dangerous (> DISK_LIMIT_PERCENT)"""
    total, used, free = shutil.disk_usage("/")
    percent = (used / total) * 100
    return percent > DISK_LIMIT_PERCENT


def handle_remove_readonly(func, path, exc):
    """Force deletes read-only files"""
    excvalue = exc[1]
    if func in (os.rmdir, os.remove, os.unlink) and excvalue.errno == 13:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    else:
        raise


def list_s3_zips_for_folder(folder_name):
    """
    Lists all zip files on S3 for a given folder.
    Returns them grouped and sorted so splits are in order.
    """
    s3 = get_s3_client()
    clean_name = folder_name.replace(" ", "_")
    prefix = f"{S3_PREFIX}{clean_name}_"

    all_keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.zip'):
                all_keys.append(key)

    def natural_sort_key(s):
        return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]

    all_keys.sort(key=natural_sort_key)
    return all_keys


def download_unzip_upload_one(s3_key, folder_name, status_name, status_queue):
    """
    Downloads a single zip from S3, unzips it into a temp dir,
    uploads it to the target remote via rclone, then cleans up.
    This processes one zip at a time to minimize disk usage.
    Returns True on success.
    """
    s3 = get_s3_client()
    zip_filename = s3_key.split('/')[-1]
    local_zip = f"/content/{zip_filename}_{random.randint(1000,9999)}"
    temp_unzip_dir = f"/content/unzip_{zip_filename}_{random.randint(1000,9999)}"

    try:
        os.makedirs(temp_unzip_dir, exist_ok=True)

        # 1. Download zip from S3
        status_queue.put((status_name, "DOWNLOADING", zip_filename))
        s3.download_file(S3_BUCKET, s3_key, local_zip)
        file_size_mb = os.path.getsize(local_zip) / (1024 * 1024)
        status_queue.put((status_name, "DOWNLOADED", f"{int(file_size_mb)} MB"))

        # 2. Unzip
        status_queue.put((status_name, "UNZIPPING", zip_filename))
        cmd_unzip = ["unzip", "-o", "-q", local_zip, "-d", temp_unzip_dir]
        result = subprocess.run(cmd_unzip, capture_output=True, text=True)

        if result.returncode not in (0, 1):  # 1 = warnings (OK)
            status_queue.put((status_name, "WARN", f"unzip rc={result.returncode}"))

        # Delete zip immediately to free disk
        if os.path.exists(local_zip):
            os.remove(local_zip)

        # Count files
        total_files = sum(len(files) for _, _, files in os.walk(temp_unzip_dir))
        total_size = int(get_folder_size_mb(temp_unzip_dir))
        status_queue.put((status_name, "UNZIPPED", f"{total_files} files, {total_size} MB"))

        # 3. Upload or move locally
        if SKIP_UPLOAD:
            final_dir = os.path.join(LOCAL_OUTPUT_DIR, folder_name)
            os.makedirs(final_dir, exist_ok=True)
            # Merge into local output
            cmd_merge = f'cp -rn "{temp_unzip_dir}/." "{final_dir}/"'
            subprocess.run(cmd_merge, shell=True)
            status_queue.put((status_name, "SAVED", f"Local: {final_dir}"))
        else:
            target = f"{TARGET_REMOTE}{TARGET_PATH}/{folder_name}"
            status_queue.put((status_name, "UPLOADING", f"→ {target}"))

            cmd_upload = [
                'rclone', 'copy',
                temp_unzip_dir, target,
                '--config=/content/rclone.conf',
                f'--transfers={UPLOAD_THREADS}',
                '--ignore-errors',
                '--quiet'
            ]

            proc = subprocess.Popen(cmd_upload, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            while proc.poll() is None:
                status_queue.put((status_name, "UPLOADING", f"{total_files} files → remote"))
                time.sleep(3)

            if proc.returncode != 0:
                err = ""
                try:
                    err = proc.stderr.read().decode('utf-8', errors='replace')[:100]
                except:
                    pass
                status_queue.put((status_name, "UPLOAD_ERR", err[:40]))
                return False

            status_queue.put((status_name, "UPLOADED", f"→ {target}"))

        # 4. Save progress to S3
        mark_zip_processed(folder_name, s3_key)
        status_queue.put((status_name, "COMPLETED", "Progress saved ✓"))
        return True

    except Exception as e:
        status_queue.put((status_name, "ERROR", str(e)[:40]))
        return False

    finally:
        # Cleanup
        if os.path.exists(local_zip):
            try:
                os.remove(local_zip)
            except:
                pass
        if os.path.exists(temp_unzip_dir):
            try:
                shutil.rmtree(temp_unzip_dir, onerror=handle_remove_readonly)
            except:
                subprocess.run(["rm", "-rf", temp_unzip_dir])


def process_folder(args):
    """
    Main worker for one subfolder:
    1. List all zip parts on S3
    2. Skip already-processed zips (resume)
    3. For each remaining zip: download → unzip → upload → cleanup → save progress
    """
    folder_name, status_queue = args

    try:
        # Check if fully done already
        if is_folder_complete(folder_name):
            status_queue.put((folder_name, "SKIPPED", "Fully done (resumed)"))
            return

        # 1. List all zips for this folder
        status_queue.put((folder_name, "SCANNING", "Listing S3 zips..."))
        zip_keys = list_s3_zips_for_folder(folder_name)

        if not zip_keys:
            status_queue.put((folder_name, "SKIPPED", "No zips found on S3"))
            return

        # 2. Filter out already-processed zips
        processed = get_processed_keys(folder_name)
        remaining_keys = [k for k in zip_keys if k not in processed]

        if processed:
            skipped = len(zip_keys) - len(remaining_keys)
            status_queue.put((folder_name, "RESUMED", f"Skipped {skipped}/{len(zip_keys)} done zips"))

        if not remaining_keys:
            status_queue.put((folder_name, "COMPLETED", "All zips already processed"))
            mark_folder_complete(folder_name)
            return

        status_queue.put((folder_name, "FOUND", f"{len(remaining_keys)} zip(s) remaining"))

        # 3. Process each zip one at a time (download → unzip → upload → clean)
        for i, s3_key in enumerate(remaining_keys):
            part_label = f"{folder_name}[{i+1}/{len(remaining_keys)}]"

            # Check disk before processing
            if check_disk_usage():
                status_queue.put((folder_name, "DISK WARN", "High disk, cleaning..."))
                # Force garbage collection of temp files
                for item in os.listdir("/content"):
                    if item.startswith("unzip_") or item.startswith("merge_"):
                        try:
                            shutil.rmtree(os.path.join("/content", item), onerror=handle_remove_readonly)
                        except:
                            pass

            success = download_unzip_upload_one(s3_key, folder_name, part_label, status_queue)
            if not success:
                status_queue.put((folder_name, "WARN", f"Failed: {s3_key.split('/')[-1]}, continuing..."))
                # Continue with remaining zips

        # 4. Mark folder complete
        mark_folder_complete(folder_name)
        status_queue.put((folder_name, "COMPLETED", "All zips processed ✓"))

    except Exception as e:
        status_queue.put((folder_name, "ERROR", str(e)[:40]))


def monitor(queue, total_folders):
    """Live status monitor with color-coded output"""
    statuses = {}
    print("\n" * (total_folders + 5))
    while True:
        while not queue.empty():
            part, state, info = queue.get()
            statuses[part] = (state, info)

        sys.stdout.write(f"\033[{len(statuses)+5}A")
        print(f"{'FOLDER/PART':<30} | {'STATUS':<15} | {'INFO':<35}\n" + "-" * 85)

        done = 0

        def natural_sort_key(s):
            return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]

        sorted_keys = sorted(statuses.keys(), key=natural_sort_key)

        for p in sorted_keys:
            state, info = statuses[p]
            if state in ["COMPLETED", "SKIPPED", "ERROR"]:
                done += 1
            if state == "ERROR":
                row = f"\033[91m{p:<30} | {state:<15} | {info:<35}\033[0m"
            elif state in ["COMPLETED", "SKIPPED"]:
                row = f"\033[92m{p:<30} | {state:<15} | {info:<35}\033[0m"
            elif state == "RESUMED":
                row = f"\033[96m{p:<30} | {state:<15} | {info:<35}\033[0m"
            elif "DISK" in state:
                row = f"\033[93m{p:<30} | {state:<15} | {info:<35}\033[0m"
            elif state in ["UPLOADING", "UPLOADED"]:
                row = f"\033[96m{p:<30} | {state:<15} | {info:<35}\033[0m"
            else:
                row = f"{p:<30} | {state:<15} | {info:<35}"
            print(row)

        sys.stdout.flush()
        time.sleep(1)


def main():
    print("📦 PYTHON UNZIPPER & MERGER (Colab + S3 Resume)")
    print("=" * 55)
    print(f"   S3 Bucket  : {S3_BUCKET}")
    print(f"   S3 Prefix  : {S3_PREFIX}")
    if SKIP_UPLOAD:
        print(f"   Output     : LOCAL → {LOCAL_OUTPUT_DIR}")
    else:
        print(f"   Target     : {TARGET_REMOTE}{TARGET_PATH}")
    print(f"   Workers    : {MAX_PARALLEL_WORKERS}")
    print("=" * 55)

    # 1. Install dependencies
    print("\n🛠️  Checking dependencies...")
    subprocess.run(
        "apt-get update && apt-get install -y unzip",
        shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    if shutil.which("rclone") is None and not SKIP_UPLOAD:
        print("   ⬇️  Installing Rclone...")
        subprocess.run(
            "curl https://rclone.org/install.sh | sudo bash",
            shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )

    if not SKIP_UPLOAD and not os.path.exists("/content/rclone.conf"):
        print("⚠️  WARNING: /content/rclone.conf not found!")
        print("   Please upload your rclone.conf or configure rclone first.")
        print("   Or set SKIP_UPLOAD = True to only extract locally.\n")

    print("✅ Dependencies ready!\n")

    # 2. Load progress from S3
    print("📋 Loading progress from S3...")
    progress = load_progress()
    if progress:
        for fname, pdata in progress.items():
            done_keys = len(pdata.get("processed_keys", []))
            is_done = pdata.get("folder_complete", False)
            status = "✅ COMPLETE" if is_done else f"⏳ {done_keys} zips processed"
            print(f"   {fname}: {status}")
    else:
        print("   No previous progress found. Starting fresh.")
    print()

    # 3. Process each subfolder
    m = multiprocessing.Manager()
    q = m.Queue()

    tasks = [(folder, q) for folder in SUBFOLDERS]

    # Start monitor
    monitor_thread = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    monitor_thread.submit(monitor, q, len(SUBFOLDERS))

    # Process folders in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as exe:
        exe.map(process_folder, tasks)

    print("\n\n🏁 ALL DONE!")
    if SKIP_UPLOAD:
        print(f"   Files extracted to: {LOCAL_OUTPUT_DIR}")
    else:
        print(f"   Files uploaded to: {TARGET_REMOTE}{TARGET_PATH}")


if __name__ == "__main__":
    main()
