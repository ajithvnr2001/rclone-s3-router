#!/usr/bin/env python3
"""
PYTHON MASTER WORKER (Final Version + S3 Resume + Large File Handling)
Features:
- Auto-dependency install (Rclone/Zip)
- Smart Disk Splitting (Never runs out of space)
- Max zip size cap (20GB default — triggers split before exceeding)
- Robust Cleanup (Force deletes locked folders)
- S3 Progress Tracking (Saves progress JSON to S3 after every completed part)
- Crash Resume (Reads progress from S3 on startup, skips completed work)
- Large File Direct Transfer (files > threshold copied directly source → destination)
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
SOURCE = "onedrive:Work Files"      # rclone remote:path (source to zip from)
DESTINATION = "gdrive:Work Files"   # rclone remote:path (destination for large files)
S3_BUCKET = "workfiles123"
S3_PREFIX = "work_files_zips/"

AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
S3_ENDPOINT = "https://s3.ap-northeast-1.wasabisys.com"

# Tuning
MAX_PARALLEL_WORKERS = 2    # Number of simultaneous parts (Colab limit: 2 recommended)
DOWNLOAD_THREADS = 6        # Rclone transfers per worker
SPLIT_THRESHOLD = 1000      # Files per batch
DISK_LIMIT_PERCENT = 80     # Trigger split/clean cycle at 80% disk usage
MAX_ZIP_SIZE_GB = 20        # Max zip size in GB — triggers split when download exceeds this
# =======================================

MAX_ZIP_SIZE_BYTES = MAX_ZIP_SIZE_GB * 1024 * 1024 * 1024

# ============ S3 FOLDER INDEX ============
FOLDER_INDEX_KEY = f"{S3_PREFIX}_index/folder_list.txt"

# ============ S3 PROGRESS TRACKING ============
PROGRESS_KEY = f"{S3_PREFIX}_progress/zipper_progress.json"

def get_s3_client():
    return boto3.client(
        service_name='s3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url=S3_ENDPOINT
    )

def fetch_folder_list():
    """Fetch the folder list from S3 (created by mapper.py)."""
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=FOLDER_INDEX_KEY)
        content = response['Body'].read().decode('utf-8')
        folders = [line.strip() for line in content.splitlines() if line.strip()]
        print(f"   📁 Found {len(folders)} folders from S3 index")
        return folders
    except Exception as e:
        print(f"   ❌ Could not fetch folder index from S3: {e}")
        print(f"   💡 Run mapper.py first to create the folder index.")
        return []

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

def mark_part_complete(folder_name, s3_key, files_in_part):
    progress = load_progress()
    if folder_name not in progress:
        progress[folder_name] = {"completed_keys": [], "completed_files": [], "large_files_done": []}
    if s3_key not in progress[folder_name]["completed_keys"]:
        progress[folder_name]["completed_keys"].append(s3_key)
    progress[folder_name]["completed_files"].extend(files_in_part)
    progress[folder_name]["completed_files"] = list(set(progress[folder_name]["completed_files"]))
    save_progress(progress)

def mark_large_file_complete(folder_name, file_path):
    """Mark a single large file as transferred."""
    progress = load_progress()
    if folder_name not in progress:
        progress[folder_name] = {"completed_keys": [], "completed_files": [], "large_files_done": []}
    if "large_files_done" not in progress[folder_name]:
        progress[folder_name]["large_files_done"] = []
    if file_path not in progress[folder_name]["large_files_done"]:
        progress[folder_name]["large_files_done"].append(file_path)
    save_progress(progress)

def mark_folder_complete(folder_name):
    progress = load_progress()
    if folder_name not in progress:
        progress[folder_name] = {"completed_keys": [], "completed_files": [], "large_files_done": []}
    progress[folder_name]["folder_complete"] = True
    save_progress(progress)

def get_completed_files(folder_name):
    progress = load_progress()
    if folder_name in progress:
        return set(progress[folder_name].get("completed_files", []))
    return set()

def get_completed_large_files(folder_name):
    progress = load_progress()
    if folder_name in progress:
        return set(progress[folder_name].get("large_files_done", []))
    return set()

def is_folder_complete(folder_name):
    progress = load_progress()
    return progress.get(folder_name, {}).get("folder_complete", False)

def is_key_complete(folder_name, s3_key):
    progress = load_progress()
    return s3_key in progress.get(folder_name, {}).get("completed_keys", [])

# ============ UTILITY FUNCTIONS ============

def get_folder_size_mb(path):
    total_size = 0
    try:
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    except: pass
    return total_size / (1024 * 1024)

def get_folder_size_bytes(path):
    total_size = 0
    try:
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    except: pass
    return total_size

def check_disk_usage():
    total, used, free = shutil.disk_usage("/")
    percent = (used / total) * 100
    return percent > DISK_LIMIT_PERCENT

def handle_remove_readonly(func, path, exc):
    excvalue = exc[1]
    if func in (os.rmdir, os.remove, os.unlink) and excvalue.errno == 13:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    else:
        raise

def fetch_map(folder_name):
    """Downloads the normal file list from S3 (excludes large files)."""
    clean_name = folder_name.replace(" ", "_")
    map_key = f"{S3_PREFIX}{clean_name}_List.txt"
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=map_key)
        content = response['Body'].read().decode('utf-8')
        return [line.strip() for line in content.splitlines() if line.strip()]
    except:
        return []

def fetch_large_files(folder_name):
    """Downloads the large files list from S3."""
    clean_name = folder_name.replace(" ", "_")
    large_key = f"{S3_PREFIX}{clean_name}_LargeFiles.json"
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=large_key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except:
        return []

# ============ LARGE FILE DIRECT TRANSFER ============

def transfer_large_files(folder_name, status_queue):
    """
    Transfer large files directly from SOURCE to DESTINATION via rclone.
    No zipping, no S3 — direct server-side copy preserving path structure.
    Runs in parallel with normal zip processing.
    """
    large_files = fetch_large_files(folder_name)
    if not large_files:
        return

    # Filter out already-completed large files
    done = get_completed_large_files(folder_name)
    remaining = [lf for lf in large_files if lf['path'] not in done]

    if not remaining:
        status_queue.put((f"⚡{folder_name}", "SKIPPED", "All large files done"))
        return

    status_queue.put((f"⚡{folder_name}", "LARGE FILES", f"{len(remaining)} file(s)"))

    for i, lf in enumerate(remaining):
        file_path = lf['path']
        size_gb = lf.get('size_gb', '?')
        label = f"⚡{folder_name}[{i+1}/{len(remaining)}]"

        status_queue.put((label, "DIRECT COPY", f"{file_path} ({size_gb} GB)"))

        # Direct rclone copyto: source file → destination file
        src = f"{SOURCE}/{folder_name}/{file_path}"
        dst = f"{DESTINATION}/{folder_name}/{file_path}"

        cmd = [
            'rclone', 'copyto', src, dst,
            '--config=/content/rclone.conf',
            '--ignore-errors',
            '--quiet'
        ]

        proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        while proc.poll() is None:
            status_queue.put((label, "TRANSFERRING", f"{file_path} ({size_gb} GB)"))
            time.sleep(5)

        if proc.returncode == 0:
            mark_large_file_complete(folder_name, file_path)
            status_queue.put((label, "COMPLETED", f"✓ {file_path}"))
        else:
            err = ""
            try: err = proc.stderr.read().decode('utf-8', errors='replace')[:60]
            except: pass
            status_queue.put((label, "ERROR", f"{file_path}: {err[:30]}"))

# ============ NORMAL ZIP PIPELINE ============

def pipeline_worker(task_data):
    """
    The Core Logic for normal files (≤ threshold):
    1. Checks dependencies.
    2. Loops through files.
    3. Monitor disk + zip size (20GB cap).
    4. Splits if disk full or zip would exceed max size.
    5. Saves progress to S3 after each completed part.
    """
    (original_file_list, folder_path, base_s3_key, part_name, folder_name, status_queue) = task_data
    s3 = get_s3_client()

    if shutil.which("rclone") is None:
        status_queue.put((part_name, "ERROR", "Rclone Missing"))
        return
    if shutil.which("zip") is None:
        status_queue.put((part_name, "ERROR", "Zip Missing"))
        return

    # Resume: filter out already completed files
    completed_files = get_completed_files(folder_name)
    if completed_files:
        before = len(original_file_list)
        original_file_list = [f for f in original_file_list if f not in completed_files]
        skipped = before - len(original_file_list)
        if skipped > 0:
            status_queue.put((part_name, "RESUMED", f"Skipped {skipped} done files"))
        if not original_file_list:
            status_queue.put((part_name, "SKIPPED", "All files done (resumed)"))
            return

    remaining_files = original_file_list[:]
    split_index = 0

    # Check if full part already exists
    if is_key_complete(folder_name, base_s3_key):
        status_queue.put((part_name, "SKIPPED", "Exists in progress"))
        return
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=base_s3_key)
        status_queue.put((part_name, "SKIPPED", "Exists on S3"))
        mark_part_complete(folder_name, base_s3_key, original_file_list)
        return
    except:
        pass

    # === SMART LOOP ===
    while len(remaining_files) > 0:
        if split_index == 0:
            current_s3_key = base_s3_key
            current_status_name = part_name
        else:
            ext = base_s3_key.split('.')[-1]
            base = base_s3_key.replace(f".{ext}", "")
            current_s3_key = f"{base}_Split{split_index}.{ext}"
            current_status_name = f"{part_name}.{split_index}"

        # Resume check for split
        if is_key_complete(folder_name, current_s3_key):
            status_queue.put((current_status_name, "SKIPPED", "Split exists (resumed)"))
            split_index += 1
            continue
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=current_s3_key)
            status_queue.put((current_status_name, "SKIPPED", "Split on S3"))
            split_index += 1
            continue
        except:
            pass

        temp_dir = f"/content/temp_{part_name}_{split_index}_{random.randint(1000,9999)}"
        zip_filename = current_s3_key.split('/')[-1]
        local_zip = f"/content/{zip_filename}"
        proc = None
        disk_triggered = False
        size_triggered = False

        try:
            os.makedirs(temp_dir, exist_ok=True)

            list_path = f"{temp_dir}/filelist.txt"
            with open(list_path, 'w') as f:
                for item in remaining_files: f.write(f"{item}\n")

            status_queue.put((current_status_name, "DOWNLOADING", f"Target: {len(remaining_files)} files"))
            cmd_dl = ['rclone', 'copy', folder_path, temp_dir, '--files-from', list_path,
                      '--config=/content/rclone.conf', f'--transfers={DOWNLOAD_THREADS}',
                      '--ignore-errors', '--no-traverse', '--quiet']

            proc = subprocess.Popen(cmd_dl, stderr=subprocess.PIPE)

            # === MONITOR LOOP (disk + size guard) ===
            while proc.poll() is None:
                size_mb = int(get_folder_size_mb(temp_dir))
                size_bytes = get_folder_size_bytes(temp_dir)

                # DISK GUARD
                if check_disk_usage():
                    status_queue.put((current_status_name, "DISK FULL", "Halting & Splitting"))
                    proc.kill()
                    disk_triggered = True
                    break

                # ZIP SIZE GUARD (20GB cap)
                if size_bytes > MAX_ZIP_SIZE_BYTES:
                    status_queue.put((current_status_name, "SIZE CAP", f"{MAX_ZIP_SIZE_GB}GB limit hit"))
                    proc.kill()
                    size_triggered = True
                    break

                status_queue.put((current_status_name, "DOWNLOADING", f"{size_mb} MB / {MAX_ZIP_SIZE_GB*1024} MB max"))
                time.sleep(2)

            if disk_triggered or size_triggered:
                time.sleep(2)

            # === INVENTORY CHECK ===
            downloaded_files = []
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if file == "filelist.txt": continue
                    abs_path = os.path.join(root, file)
                    rel_path = os.path.relpath(abs_path, temp_dir)
                    downloaded_files.append(rel_path)

            downloaded_set = set(downloaded_files)
            new_remaining = []
            for f in remaining_files:
                norm_f = f.replace('\\', '/')
                if norm_f not in downloaded_set and f not in downloaded_set:
                    new_remaining.append(f)

            remaining_files = new_remaining

            # === ZIP & UPLOAD ===
            if downloaded_files:
                status_queue.put((current_status_name, "ZIPPING", f"{len(downloaded_files)} files"))
                if os.path.exists(list_path): os.remove(list_path)

                cmd_zip = ["zip", "-0", "-r", "-q", local_zip, "."]
                subprocess.run(cmd_zip, cwd=temp_dir)

                if os.path.exists(local_zip):
                    file_size = os.path.getsize(local_zip)
                    status_queue.put((current_status_name, "UPLOADING", f"{int(file_size/(1024*1024))} MB"))
                    s3.upload_file(local_zip, S3_BUCKET, current_s3_key)
                    mark_part_complete(folder_name, current_s3_key, downloaded_files)
                    status_queue.put((current_status_name, "COMPLETED", "Saved to S3 ✓"))
                else:
                    raise Exception(f"Zip file {zip_filename} not found")
            else:
                 if not disk_triggered and not size_triggered and proc.returncode != 0:
                     err_msg = "Rclone Failed"
                     if proc.stderr:
                         try:
                             err_output = proc.stderr.read().decode('utf-8', errors='replace')[:200]
                             if err_output.strip():
                                 err_msg = f"Rclone: {err_output.strip()[:40]}"
                         except: pass
                     status_queue.put((current_status_name, "ERROR", err_msg))
                     break

        except Exception as e:
            status_queue.put((current_status_name, "ERROR", str(e)[:20]))
            break

        finally:
            if proc and proc.poll() is None:
                try: proc.kill()
                except: pass
            if os.path.exists(local_zip):
                try: os.remove(local_zip)
                except: pass
            if os.path.exists(temp_dir):
                try: shutil.rmtree(temp_dir, onerror=handle_remove_readonly)
                except: subprocess.run(["rm", "-rf", temp_dir])

        if len(remaining_files) > 0:
            split_index += 1
            trigger = "size cap" if size_triggered else "disk"
            status_queue.put((part_name, "SPLITTING", f"{len(remaining_files)} remain ({trigger})"))
        else:
            break

# ============ MONITOR ============

def monitor(queue, num_parts):
    statuses = {}
    print("\n" * (MAX_PARALLEL_WORKERS + 5))
    while True:
        while not queue.empty():
            part, state, info = queue.get()
            statuses[part] = (state, info)

        sys.stdout.write(f"\033[{len(statuses)+5}A")
        print(f"{'PART':<20} | {'STATUS':<15} | {'INFO':<30}\n" + "-"*70)

        done = 0
        def natural_sort_key(s):
            return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]
        sorted_keys = sorted(statuses.keys(), key=natural_sort_key)

        for p in sorted_keys:
            state, info = statuses[p]
            if state in ["COMPLETED", "SKIPPED", "ERROR"]: done += 1
            if state == "ERROR": row = f"\033[91m{p:<20} | {state:<15} | {info:<30}\033[0m"
            elif state in ["COMPLETED", "SKIPPED"]: row = f"\033[92m{p:<20} | {state:<15} | {info:<30}\033[0m"
            elif state == "RESUMED": row = f"\033[96m{p:<20} | {state:<15} | {info:<30}\033[0m"
            elif state in ["DIRECT COPY", "TRANSFERRING"]: row = f"\033[95m{p:<20} | {state:<15} | {info:<30}\033[0m"
            elif "DISK FULL" in state or "SIZE CAP" in state: row = f"\033[93m{p:<20} | {state:<15} | {info:<30}\033[0m"
            else: row = f"{p:<20} | {state:<15} | {info:<30}"
            print(row)

        sys.stdout.flush()
        time.sleep(1)

# ============ MAIN ============

def main():
    print("🚀 PYTHON MASTER WORKER (Disk-Smart + S3 Resume + Large Files)")
    print("=" * 60)
    print(f"   Source       : {SOURCE}")
    print(f"   Destination  : {DESTINATION} (large files only)")
    print(f"   S3 Bucket    : {S3_BUCKET}")
    print(f"   Max Zip Size : {MAX_ZIP_SIZE_GB} GB")
    print("=" * 60)
    print("\n🛠️  Checking dependencies...")

    subprocess.run("apt-get update && apt-get install -y zip", shell=True, stdout=subprocess.DEVNULL)
    if shutil.which("rclone") is None:
        print("   ⬇️  Installing Rclone...")
        subprocess.run("curl https://rclone.org/install.sh | sudo bash", shell=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print("✅ Dependencies ready!\n")

    # Load progress
    print("📋 Loading progress from S3...")
    progress = load_progress()
    if progress:
        for fname, pdata in progress.items():
            done_keys = len(pdata.get("completed_keys", []))
            done_files = len(pdata.get("completed_files", []))
            done_large = len(pdata.get("large_files_done", []))
            is_done = pdata.get("folder_complete", False)
            status = "✅ COMPLETE" if is_done else f"⏳ {done_keys} parts, {done_files} files, {done_large} large"
            print(f"   {fname}: {status}")
    else:
        print("   No previous progress found. Starting fresh.")
    print()

    # Fetch folder list
    print("📁 Fetching folder list from S3...")
    SUBFOLDERS = fetch_folder_list()
    if not SUBFOLDERS:
        print("❌ No folders to process. Run mapper.py first!")
        return
    print()

    for folder in SUBFOLDERS:
        if is_folder_complete(folder):
            print(f"⏭️  Skipping {folder} (fully completed)")
            continue

        print(f"📦 Processing: {folder}")

        # === NORMAL FILES (zip pipeline) ===
        files = fetch_map(folder)
        has_normal = bool(files)

        # === LARGE FILES (direct transfer) ===
        large_files = fetch_large_files(folder)
        has_large = bool(large_files)

        if not has_normal and not has_large:
            print("   ⚠️  No files found on S3. Skipping.")
            continue

        # Filter completed normal files
        if has_normal:
            completed = get_completed_files(folder)
            original_count = len(files)
            files = [f for f in files if f not in completed]
            if completed:
                print(f"   ♻️  Normal: {original_count - len(files)} done, {len(files)} remaining")

        # Filter completed large files
        if has_large:
            done_large = get_completed_large_files(folder)
            remaining_large = [lf for lf in large_files if lf['path'] not in done_large]
            if done_large:
                print(f"   ♻️  Large: {len(done_large)} done, {len(remaining_large)} remaining")
        else:
            remaining_large = []

        if not files and not remaining_large:
            print(f"   ✅ All files completed!")
            mark_folder_complete(folder)
            continue

        m = multiprocessing.Manager()
        q = m.Queue()

        # Start monitor
        total_parts = math.ceil(len(files) / SPLIT_THRESHOLD) if files else 0
        monitor_thread = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        monitor_thread.submit(monitor, q, total_parts + (1 if remaining_large else 0))

        # Run normal zip tasks and large file transfer in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS + 1) as thread_exe:
            futures = []

            # Submit large file transfer (runs alongside normal zipping)
            if remaining_large:
                print(f"   ⚡ {len(remaining_large)} large file(s) → direct transfer to {DESTINATION}")
                futures.append(thread_exe.submit(transfer_large_files, folder, q))

            # Submit normal zip pipeline
            if files:
                num_parts = math.ceil(len(files) / SPLIT_THRESHOLD)
                print(f"   🔹 {len(files)} normal files → {num_parts} part(s)")

                def run_zip_pipeline():
                    tasks = []
                    for i in range(num_parts):
                        batch = files[i*SPLIT_THRESHOLD:(i+1)*SPLIT_THRESHOLD]
                        part = f"Part{i+1}" if num_parts > 1 else "Full"
                        s3_key = f"{S3_PREFIX}{folder.replace(' ','_')}_{part}.zip"
                        tasks.append((batch, f"{SOURCE}/{folder}", s3_key, part, folder, q))

                    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as exe:
                        exe.map(pipeline_worker, tasks)

                futures.append(thread_exe.submit(run_zip_pipeline))

            # Wait for all
            for f in futures:
                f.result()

        mark_folder_complete(folder)
        print(f"\n✅ {folder} — ALL DONE\n")

    print("\n🏁 ALL FOLDERS COMPLETE!")

if __name__ == "__main__":
    main()