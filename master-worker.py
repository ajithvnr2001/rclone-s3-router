#!/usr/bin/env python3
"""
PYTHON MASTER WORKER (Final Version)
Features:
- Auto-dependency install (Rclone/Zip)
- Smart Disk Splitting (Never runs out of space)
- Robust Cleanup (Force deletes locked folders)
- Resume Capability (Skips existing S3 files)
"""

import subprocess
import sys
import time
import boto3
import math
import concurrent.futures
import multiprocessing
import os
import shutil
import stat
import random
import re

# ============ CONFIGURATION ============
SUBFOLDERS = [
     "Pryor & Morrow Projects","My Companies"
]

ONEDRIVE_REMOTE = "onedrive:"
SOURCE_PATH = "Work Files"
S3_BUCKET = "workfiles123"
S3_PREFIX = "work_files_zips/"

AWS_ACCESS_KEY = "key"
AWS_SECRET_KEY = "keyid"
S3_ENDPOINT = "https://s3.ap-northeast-1.wasabisys.com"

# Tuning
MAX_PARALLEL_WORKERS = 2    # Number of simultaneous parts (Colab limit: 2 recommended)
DOWNLOAD_THREADS = 6        # Rclone transfers per worker
SPLIT_THRESHOLD = 1000      # Files per batch
DISK_LIMIT_PERCENT = 80     # Trigger split/clean cycle at 80% disk usage
# =======================================

def get_s3_client():
    return boto3.client(
        service_name='s3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url=S3_ENDPOINT
    )

def get_folder_size_mb(path):
    """Calculates folder size safely"""
    total_size = 0
    try:
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    except: pass
    return total_size / (1024 * 1024)

def check_disk_usage():
    """Returns True if disk usage is dangerous (> DISK_LIMIT_PERCENT)"""
    total, used, free = shutil.disk_usage("/")
    percent = (used / total) * 100
    return percent > DISK_LIMIT_PERCENT

def handle_remove_readonly(func, path, exc):
    """Force deletes read-only files (Fixes 'Permission Denied' errors)"""
    excvalue = exc[1]
    if func in (os.rmdir, os.remove, os.unlink) and excvalue.errno == 13:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    else:
        raise

def fetch_map(folder_name):
    """Downloads the file list from S3"""
    clean_name = folder_name.replace(" ", "_")
    map_key = f"{S3_PREFIX}{clean_name}_List.txt"
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=map_key)
        content = response['Body'].read().decode('utf-8')
        return [line.strip() for line in content.splitlines() if line.strip()]
    except:
        return []

def pipeline_worker(task_data):
    """
    The Core Logic:
    1. Checks dependencies.
    2. loops through files.
    3. Monitor disk.
    4. Splits if disk is full.
    """
    (original_file_list, folder_path, base_s3_key, part_name, status_queue) = task_data
    s3 = get_s3_client()

    # === SAFEGUARD: DEPENDENCY CHECK ===
    if shutil.which("rclone") is None:
        status_queue.put((part_name, "ERROR", "Rclone Missing"))
        return
    if shutil.which("zip") is None:
        status_queue.put((part_name, "ERROR", "Zip Missing"))
        return

    remaining_files = original_file_list[:]
    split_index = 0

    # === CHECK IF FULL PART ALREADY EXISTS ===
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=base_s3_key)
        status_queue.put((part_name, "SKIPPED", "Exists on S3"))
        return
    except:
        pass

    # === SMART LOOP ===
    while len(remaining_files) > 0:
        # Determine Filename (Standard vs Split)
        if split_index == 0:
            current_s3_key = base_s3_key
            current_status_name = part_name
        else:
            ext = base_s3_key.split('.')[-1]
            base = base_s3_key.replace(f".{ext}", "")
            current_s3_key = f"{base}_Split{split_index}.{ext}"
            current_status_name = f"{part_name}.{split_index}"

        # Unique Temp Directory
        temp_dir = f"/content/temp_{part_name}_{split_index}_{random.randint(1000,9999)}"
        zip_filename = current_s3_key.split('/')[-1]
        local_zip = f"/content/{zip_filename}"
        proc = None
        disk_triggered = False

        try:
            os.makedirs(temp_dir, exist_ok=True)

            # Write File List
            list_path = f"{temp_dir}/filelist.txt"
            with open(list_path, 'w') as f:
                for item in remaining_files: f.write(f"{item}\n")

            # === START DOWNLOAD ===
            status_queue.put((current_status_name, "DOWNLOADING", f"Target: {len(remaining_files)} files"))
            cmd_dl = ['rclone', 'copy', folder_path, temp_dir, '--files-from', list_path, '--config=/content/rclone.conf', f'--transfers={DOWNLOAD_THREADS}', '--ignore-errors', '--no-traverse', '--quiet']

            proc = subprocess.Popen(cmd_dl, stderr=subprocess.PIPE)

            # === MONITOR LOOP ===
            while proc.poll() is None:
                size_mb = int(get_folder_size_mb(temp_dir))

                # DISK GUARD
                if check_disk_usage():
                    status_queue.put((current_status_name, "DISK FULL", "Halting & Splitting"))
                    proc.kill() # Stop download immediately
                    disk_triggered = True
                    break

                status_queue.put((current_status_name, "DOWNLOADING", f"{size_mb} MB"))
                time.sleep(2)

            if disk_triggered:
                time.sleep(2) # Allow file handles to close

            # === INVENTORY CHECK ===
            downloaded_files = []
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if file == "filelist.txt": continue
                    abs_path = os.path.join(root, file)
                    rel_path = os.path.relpath(abs_path, temp_dir)
                    downloaded_files.append(rel_path)

            # Calculate what is left
            downloaded_set = set(downloaded_files)
            # Filter remaining_files to exclude what we just got
            new_remaining = []
            for f in remaining_files:
                # We normalize paths to ensure matching works across OS types
                norm_f = f.replace('\\', '/')
                if norm_f not in downloaded_set and f not in downloaded_set:
                    new_remaining.append(f)

            remaining_files = new_remaining

            # === ZIP & UPLOAD ===
            if downloaded_files:
                status_queue.put((current_status_name, "ZIPPING", f"{len(downloaded_files)} files"))
                if os.path.exists(list_path): os.remove(list_path)

                # Use list-based subprocess.run to avoid shell issues with '&' and other special chars
                cmd_zip = ["zip", "-0", "-r", "-q", local_zip, "."]
                subprocess.run(cmd_zip, cwd=temp_dir)

                if os.path.exists(local_zip):
                    file_size = os.path.getsize(local_zip)
                    status_queue.put((current_status_name, "UPLOADING", f"{int(file_size/(1024*1024))} MB"))
                    s3.upload_file(local_zip, S3_BUCKET, current_s3_key)
                    status_queue.put((current_status_name, "COMPLETED", "Saved to S3"))
                else:
                    raise Exception(f"Zip file {zip_filename} not found after creation attempt")
            else:
                 if not disk_triggered and proc.returncode != 0:
                     err_msg = "Rclone Failed"
                     if proc.stderr:
                         try:
                             err_output = proc.stderr.read().decode('utf-8', errors='replace')[:200]
                             if err_output.strip():
                                 err_msg = f"Rclone: {err_output.strip()[:40]}"
                         except: pass
                     status_queue.put((current_status_name, "ERROR", err_msg))
                     break # Fatal error

        except Exception as e:
            status_queue.put((current_status_name, "ERROR", str(e)[:20]))
            break

        finally:
            # === ROBUST CLEANUP ===
            if proc and proc.poll() is None:
                try: proc.kill()
                except: pass

            if os.path.exists(local_zip):
                try: os.remove(local_zip)
                except: pass

            if os.path.exists(temp_dir):
                try: shutil.rmtree(temp_dir, onerror=handle_remove_readonly)
                except: subprocess.run(["rm", "-rf", temp_dir])

        # Setup for next split
        if len(remaining_files) > 0:
            split_index += 1
            status_queue.put((part_name, "SPLITTING", f"{len(remaining_files)} remain"))
        else:
            break

def monitor(queue, num_parts):
    statuses = {}
    print("\n" * (MAX_PARALLEL_WORKERS + 2))
    while True:
        while not queue.empty():
            part, state, info = queue.get()
            statuses[part] = (state, info)

        sys.stdout.write(f"\033[{len(statuses)+5}A")
        print(f"{'PART':<12} | {'STATUS':<15} | {'INFO':<25}\n" + "-"*55)

        done = 0
        # Natural sort: Part1, Part2, ..., Part10 instead of Part1, Part10, Part2
        def natural_sort_key(s):
            return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]
        sorted_keys = sorted(statuses.keys(), key=natural_sort_key)

        for p in sorted_keys:
            state, info = statuses[p]
            if state in ["COMPLETED", "SKIPPED", "ERROR"]: done += 1
            # Color coding for better visibility
            if state == "ERROR": row = f"\033[91m{p:<12} | {state:<15} | {info:<25}\033[0m"
            elif state == "COMPLETED": row = f"\033[92m{p:<12} | {state:<15} | {info:<25}\033[0m"
            elif "DISK FULL" in state: row = f"\033[93m{p:<12} | {state:<15} | {info:<25}\033[0m"
            else: row = f"{p:<12} | {state:<15} | {info:<25}"
            print(row)

        sys.stdout.flush()
        time.sleep(1)

def main():
    print("🚀 PYTHON MASTER WORKER (Disk-Smart Edition)")
    print("🛠️  Checking dependencies...")

    # 1. Install Zip
    subprocess.run("apt-get update && apt-get install -y zip", shell=True, stdout=subprocess.DEVNULL)

    # 2. Install Rclone
    if shutil.which("rclone") is None:
        print("   ⬇️  Installing Rclone...")
        subprocess.run("curl https://rclone.org/install.sh | sudo bash", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    print("✅ Dependencies ready!\n")

    for folder in SUBFOLDERS:
        print(f"📦 Processing Map: {folder}")
        files = fetch_map(folder)
        if not files:
            print("   ⚠️  No map found on S3. Skipping.")
            continue

        num_parts = math.ceil(len(files) / SPLIT_THRESHOLD)
        print(f"   🔹 Splitting into {num_parts} parts.")

        m = multiprocessing.Manager()
        q = m.Queue()
        tasks = []
        for i in range(num_parts):
            batch = files[i*SPLIT_THRESHOLD:(i+1)*SPLIT_THRESHOLD]
            part = f"Part{i+1}" if num_parts > 1 else "Full"
            s3_key = f"{S3_PREFIX}{folder.replace(' ','_')}_{part}.zip"
            tasks.append((batch, f"{ONEDRIVE_REMOTE}{SOURCE_PATH}/{folder}", s3_key, part, q))

        monitor_thread = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        monitor_thread.submit(monitor, q, num_parts)

        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as exe:
            exe.map(pipeline_worker, tasks)

if __name__ == "__main__":
    main()
