"""
Remove Redundant Folder Script - RCLONE VERSION (ON-THE-GO + PARALLEL)

Processes folders as it discovers them - no pre-listing.
Uses parallel workers for speed.
"""

import subprocess
import os
import json
from concurrent.futures import ThreadPoolExecutor
import threading
import queue

# Rclone config path
RCLONE_CONFIG = "/content/rclone.conf"

# Rclone remotes
GDRIVE_REMOTE = "gdrive"
S3_REMOTE = "wasabi"

# Target folder on Google Drive
ROOT_FOLDER = "Data_Migration/Work Files"

# S3 bucket path for log file
S3_LOG_PATH = "data-migration-logs/redundant_folders_progress.log"

# Local temp file
LOCAL_LOG_FILE = "/tmp/redundant_folders_progress.log"

# Progress interval
PROGRESS_INTERVAL = 1000

# Workers
MAX_WORKERS = 8

# Thread lock
lock = threading.Lock()

# Counters
stats = {"fixed_files": 0, "fixed_folders": 0, "skipped": 0, "errors": 0, "checked": 0}
processed_folders = set()

# Work queue
work_queue = queue.Queue()


def run_rclone(args: list) -> tuple:
    cmd = ["rclone", "--config", RCLONE_CONFIG] + args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return result.returncode == 0, result.stdout
    except:
        return False, ""


def download_log_from_s3():
    print("Downloading log from S3...")
    run_rclone(["copy", f"{S3_REMOTE}:{S3_LOG_PATH}", os.path.dirname(LOCAL_LOG_FILE)])
    if not os.path.exists(LOCAL_LOG_FILE):
        open(LOCAL_LOG_FILE, 'w').close()


def upload_log_to_s3():
    run_rclone(["copy", LOCAL_LOG_FILE, f"{S3_REMOTE}:{os.path.dirname(S3_LOG_PATH)}"])


def load_processed_folders():
    global processed_folders
    if os.path.exists(LOCAL_LOG_FILE):
        with open(LOCAL_LOG_FILE, "r") as f:
            processed_folders = set(line.strip() for line in f if line.strip())
        print(f"Resuming: {len(processed_folders)} already processed")


def save_processed(path: str):
    with lock:
        with open(LOCAL_LOG_FILE, "a") as f:
            f.write(path + "\n")


def lsjson(path: str) -> list:
    ok, out = run_rclone(["lsjson", "--no-modtime", "--no-mimetype", f"{GDRIVE_REMOTE}:{path}"])
    try:
        return json.loads(out) if ok else []
    except:
        return []


def moveto(src: str, dst: str) -> bool:
    ok, _ = run_rclone(["moveto", f"{GDRIVE_REMOTE}:{src}", f"{GDRIVE_REMOTE}:{dst}"])
    return ok


def rmdir(path: str) -> bool:
    ok, _ = run_rclone(["rmdir", f"{GDRIVE_REMOTE}:{path}"])
    return ok


def process_folder(folder_path: str):
    """Process a folder and queue its subfolders."""
    folder_name = os.path.basename(folder_path)
    parent = os.path.dirname(folder_path)
    
    with lock:
        stats["checked"] += 1
        if stats["checked"] % PROGRESS_INTERVAL == 0:
            print(f"[{stats['checked']}] fixed:{stats['fixed_files']+stats['fixed_folders']} skip:{stats['skipped']} err:{stats['errors']}")
            upload_log_to_s3()
    
    if folder_path in processed_folders:
        with lock:
            stats["skipped"] += 1
        return
    
    contents = lsjson(folder_path)
    files = [c for c in contents if not c.get('IsDir')]
    dirs = [c for c in contents if c.get('IsDir')]
    
    # Queue subfolders for processing
    for d in dirs:
        work_queue.put(f"{folder_path}/{d['Path']}")
    
    # PATTERN 1: File
    if len(files) == 1 and len(dirs) == 0 and files[0]['Path'] == folder_name:
        src = f"{folder_path}/{files[0]['Path']}"
        tmp = f"{parent}/{folder_name}.tmp"
        dst = f"{parent}/{folder_name}"
        
        if moveto(src, tmp) and rmdir(folder_path) and moveto(tmp, dst):
            print(f"FILE: {folder_name} -> FIXED")
            with lock:
                stats["fixed_files"] += 1
        else:
            with lock:
                stats["errors"] += 1
        save_processed(folder_path)
        return
    
    # PATTERN 2: Folder
    if len(dirs) == 1 and len(files) == 0 and dirs[0]['Path'] == folder_name:
        sub = f"{folder_path}/{dirs[0]['Path']}"
        tmp = f"{parent}/{folder_name}.tmp"
        dst = f"{parent}/{folder_name}"
        
        if moveto(sub, tmp) and rmdir(folder_path) and moveto(tmp, dst):
            print(f"FOLDER: {folder_name} -> FIXED")
            with lock:
                stats["fixed_folders"] += 1
        else:
            with lock:
                stats["errors"] += 1
        save_processed(folder_path)


def worker():
    """Worker thread that processes folders from queue."""
    while True:
        try:
            folder = work_queue.get(timeout=30)
            process_folder(folder)
            work_queue.task_done()
        except queue.Empty:
            break


def main():
    download_log_from_s3()
    load_processed_folders()
    
    print(f"Starting ON-THE-GO processing with {MAX_WORKERS} workers...")
    print("-" * 50)
    
    # Seed the queue with root folder contents
    contents = lsjson(ROOT_FOLDER)
    for item in contents:
        if item.get('IsDir'):
            work_queue.put(f"{ROOT_FOLDER}/{item['Path']}")
    
    print(f"Queued {work_queue.qsize()} top-level folders")
    
    # Start workers
    threads = []
    for _ in range(MAX_WORKERS):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)
    
    # Wait for completion
    for t in threads:
        t.join()
    
    upload_log_to_s3()
    
    print(f"\n{'=' * 50}")
    print(f"DONE!")
    print(f"  Checked: {stats['checked']}")
    print(f"  Fixed: {stats['fixed_files'] + stats['fixed_folders']}")
    print(f"  Skipped: {stats['skipped']}")
    print(f"  Errors: {stats['errors']}")


if __name__ == "__main__":
    print("=" * 50)
    print("REMOVE REDUNDANT FOLDERS (ON-THE-GO)")
    print(f"Source: {GDRIVE_REMOTE}:{ROOT_FOLDER}")
    print("=" * 50)
    main()
