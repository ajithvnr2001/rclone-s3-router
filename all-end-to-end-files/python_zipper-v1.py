#!/usr/bin/env python3
"""
PYTHON MASTER WORKER (Fixed Version + S3 Resume + Large File Handling)
Features:
- Auto-dependency install (Rclone/Zip)
- Smart Disk Splitting (Never runs out of space)
- Max zip size cap (20GB default — triggers split before exceeding)
- Robust Cleanup (Force deletes locked folders)
- S3 Progress Tracking (Saves progress JSON to S3 after every completed part)
- Crash Resume (Reads progress from S3 on startup, skips completed work)
- Large File Direct Transfer (files > threshold copied directly source → destination)

FIXES APPLIED:
- Environment variables for credentials (no hardcoding)
- Null-safe lock handling
- Proper exception handling (no bare except)
- Safe S3 key encoding
- Configurable paths (not hardcoded to Colab)
- Division by zero protection
- Proper error messages
- **BUGFIX**: Added missing 'json' import
"""

import subprocess
import sys
import time
import os
import shutil
import stat
import random
import re
import math
import json  # <--- FIX: Added missing import
import concurrent.futures
import multiprocessing
from urllib.parse import quote
from typing import Optional, Set, List, Dict, Any

# Check boto3 early
try:
    import boto3
except ImportError:
    print("❌ boto3 not installed! Run: pip install boto3")
    sys.exit(1)

# ============ CONFIGURATION ============
SOURCE = "onedrive:Work Files"      # rclone remote:path (source to zip from)
DESTINATION = "gdrive:Work Files"   # rclone remote:path (destination for large files)
S3_BUCKET = os.environ.get("S3_BUCKET", "workfiles123")
S3_PREFIX = os.environ.get("S3_PREFIX", "work_files_zips/")

# Get credentials from environment variables (SECURE - never hardcode!)
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "https://s3.ap-northeast-1.wasabisys.com")

# Tuning
MAX_PARALLEL_WORKERS = 2    # Number of simultaneous parts (Colab limit: 2 recommended)
DOWNLOAD_THREADS = 6        # Rclone transfers per worker
SPLIT_THRESHOLD = 1000      # Files per batch
DISK_LIMIT_PERCENT = 80     # Trigger split/clean cycle at 80% disk usage
MAX_ZIP_SIZE_GB = 20        # Max zip size in GB — triggers split when download exceeds this

# Paths - configurable via environment
WORK_DIR = os.environ.get("WORK_DIR", "/content")
RCLONE_CONFIG = os.environ.get("RCLONE_CONFIG", "/content/rclone.conf")
# =======================================

MAX_ZIP_SIZE_BYTES = MAX_ZIP_SIZE_GB * 1024 * 1024 * 1024

# Process-safe lock — will be set in worker processes
_progress_lock: Optional[multiprocessing.Manager().Lock] = None

# ============ S3 FOLDER INDEX ============
FOLDER_INDEX_KEY = f"{S3_PREFIX}_index/folder_list.txt"

# ============ S3 PROGRESS TRACKING ============
PROGRESS_KEY = f"{S3_PREFIX}_progress/zipper_progress.json"


def sanitize_name(name: str) -> str:
    """Sanitize name for S3 key while preserving readability."""
    return quote(name, safe='').replace('%20', '_').replace('%2F', '_')


def get_s3_client():
    """Create S3 client with validation."""
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        raise ValueError(
            "AWS credentials not configured!\n"
            "Set environment variables:\n"
            "  export AWS_ACCESS_KEY_ID='your_access_key'\n"
            "  export AWS_SECRET_ACCESS_KEY='your_secret_key'"
        )
    return boto3.client(
        service_name='s3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url=S3_ENDPOINT
    )


def fetch_folder_list() -> List[str]:
    """Fetch the folder list from S3 (created by mapper.py)."""
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=FOLDER_INDEX_KEY)
        content = response['Body'].read().decode('utf-8')
        folders = [line.strip() for line in content.splitlines() if line.strip()]
        print(f"   📁 Found {len(folders)} folders from S3 index")
        return folders
    except boto3.exceptions.Boto3Error as e:
        print(f"   ❌ Could not fetch folder index from S3: {e}")
        print(f"   💡 Run mapper.py first to create the folder index.")
        return []
    except Exception as e:
        print(f"   ❌ Unexpected error fetching folder index: {e}")
        return []


def load_progress() -> Dict[str, Any]:
    """Load progress JSON from S3. Returns dict or empty dict on failure."""
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=PROGRESS_KEY)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        # FIXED: boto3.exceptions.NoSuchKey doesn't exist - use generic Exception
        # and check error message for NoSuchKey or 404 Not Found
        error_str = str(e)
        if 'NoSuchKey' in error_str or 'Not Found' in error_str or '404' in error_str:
            return {}  # No progress file yet - normal for first run
        # Other S3/network errors
        print(f"   ⚠️ Error loading progress from S3: {e}")
        return {}
    except json.JSONDecodeError as e:
        print(f"   ⚠️ Progress file corrupted, starting fresh: {e}")
        return {}


def save_progress(progress: Dict[str, Any]) -> bool:
    """Save progress JSON to S3. Returns True on success."""
    s3 = get_s3_client()
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=PROGRESS_KEY,
            Body=json.dumps(progress, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        return True
    except boto3.exceptions.Boto3Error as e:
        print(f"⚠️  Failed to save progress to S3: {e}")
        return False
    except Exception as e:
        print(f"⚠️  Unexpected error saving progress: {e}")
        return False


def _update_progress_safe(update_func) -> None:
    """Safely update progress with lock handling."""
    global _progress_lock
    
    if _progress_lock is not None:
        with _progress_lock:
            update_func()
    else:
        # No lock available (shouldn't happen in normal flow)
        update_func()


def mark_part_complete(folder_name: str, s3_key: str, files_in_part: List[str]) -> None:
    """Mark a part as complete in progress tracking."""
    def update():
        progress = load_progress()
        if folder_name not in progress:
            progress[folder_name] = {
                "completed_keys": [],
                "completed_files": [],
                "large_files_done": []
            }
        if s3_key not in progress[folder_name]["completed_keys"]:
            progress[folder_name]["completed_keys"].append(s3_key)
        
        # Use set for deduplication but keep as list for JSON
        existing = set(progress[folder_name]["completed_files"])
        existing.update(files_in_part)
        progress[folder_name]["completed_files"] = list(existing)
        
        save_progress(progress)
    
    _update_progress_safe(update)


def mark_large_file_complete(folder_name: str, file_path: str) -> None:
    """Mark a single large file as transferred."""
    def update():
        progress = load_progress()
        if folder_name not in progress:
            progress[folder_name] = {
                "completed_keys": [],
                "completed_files": [],
                "large_files_done": []
            }
        if "large_files_done" not in progress[folder_name]:
            progress[folder_name]["large_files_done"] = []
        if file_path not in progress[folder_name]["large_files_done"]:
            progress[folder_name]["large_files_done"].append(file_path)
        save_progress(progress)
    
    _update_progress_safe(update)


def mark_folder_complete(folder_name: str) -> None:
    """Mark folder as fully complete."""
    def update():
        progress = load_progress()
        if folder_name not in progress:
            progress[folder_name] = {
                "completed_keys": [],
                "completed_files": [],
                "large_files_done": []
            }
        progress[folder_name]["folder_complete"] = True
        save_progress(progress)
    
    _update_progress_safe(update)


def get_completed_files(folder_name: str) -> Set[str]:
    """Get set of completed files for a folder."""
    progress = load_progress()
    if folder_name in progress:
        return set(progress[folder_name].get("completed_files", []))
    return set()


def get_completed_large_files(folder_name: str) -> Set[str]:
    """Get set of completed large files for a folder."""
    progress = load_progress()
    if folder_name in progress:
        return set(progress[folder_name].get("large_files_done", []))
    return set()


def is_folder_complete(folder_name: str) -> bool:
    """Check if folder is marked complete."""
    progress = load_progress()
    return progress.get(folder_name, {}).get("folder_complete", False)


def is_key_complete(folder_name: str, s3_key: str) -> bool:
    """Check if specific S3 key is already processed."""
    progress = load_progress()
    return s3_key in progress.get(folder_name, {}).get("completed_keys", [])

# ============ UTILITY FUNCTIONS ============

def get_folder_size_mb(path: str) -> float:
    """Calculate folder size in MB. Returns 0.0 on error."""
    total_size = 0
    try:
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                try:
                    if not os.path.islink(fp):
                        total_size += os.path.getsize(fp)
                except (OSError, IOError):
                    continue  # Skip files we can't read
    except (OSError, IOError):
        pass
    return total_size / (1024 * 1024)


def get_folder_size_bytes(path: str) -> int:
    """Calculate folder size in bytes. Returns 0 on error."""
    total_size = 0
    try:
        for dirpath, _, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                try:
                    if not os.path.islink(fp):
                        total_size += os.path.getsize(fp)
                except (OSError, IOError):
                    continue
    except (OSError, IOError):
        pass
    return total_size


def check_disk_usage() -> bool:
    """Returns True if disk usage exceeds DISK_LIMIT_PERCENT."""
    try:
        total, used, free = shutil.disk_usage("/")
        if total > 0:
            percent = (used / total) * 100
            return percent > DISK_LIMIT_PERCENT
    except (OSError, IOError):
        pass
    return False  # Assume OK if we can't check


def handle_remove_readonly(func, path: str, exc) -> None:
    """Force delete read-only files on Windows."""
    excvalue = exc[1]
    if func in (os.rmdir, os.remove, os.unlink) and hasattr(excvalue, 'errno') and excvalue.errno == 13:
        try:
            os.chmod(path, stat.S_IWRITE)
            func(path)
        except (OSError, IOError):
            raise
    else:
        raise


def fetch_map(folder_name: str) -> List[str]:
    """Downloads the normal file list from S3 (excludes large files)."""
    safe_name = sanitize_name(folder_name)
    map_key = f"{S3_PREFIX}{safe_name}_List.txt"
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=map_key)
        content = response['Body'].read().decode('utf-8')
        return [line.strip() for line in content.splitlines() if line.strip()]
    except boto3.exceptions.NoSuchKey:
        return []
    except boto3.exceptions.Boto3Error as e:
        print(f"   ⚠️ Error fetching file map: {e}")
        return []


def fetch_large_files(folder_name: str) -> List[Dict[str, Any]]:
    """Downloads the large files list from S3."""
    safe_name = sanitize_name(folder_name)
    large_key = f"{S3_PREFIX}{safe_name}_LargeFiles.json"
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=large_key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except boto3.exceptions.NoSuchKey:
        return []
    except (boto3.exceptions.Boto3Error, json.JSONDecodeError) as e:
        print(f"   ⚠️ Error fetching large files list: {e}")
        return []

# ============ LARGE FILE DIRECT TRANSFER ============

def transfer_large_files(folder_name: str, status_queue, lock) -> List[str]:
    """
    Transfer large files directly from SOURCE to DESTINATION via rclone.
    No zipping, no S3 — direct server-side copy preserving path structure.
    Returns list of failed file paths.
    """
    global _progress_lock
    _progress_lock = lock
    
    large_files = fetch_large_files(folder_name)
    if not large_files:
        return []  # No failures, no files
    
    # Filter out already-completed large files
    done = get_completed_large_files(folder_name)
    remaining = [lf for lf in large_files if lf['path'] not in done]
    
    if not remaining:
        status_queue.put((f"⚡{folder_name}", "SKIPPED", "All large files done"))
        return []
    
    status_queue.put((f"⚡{folder_name}", "LARGE FILES", f"{len(remaining)} file(s)"))
    failed_large = []
    
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
            '--ignore-errors',
            '--quiet'
        ]
        
        if os.path.exists(RCLONE_CONFIG):
            cmd.extend(['--config', RCLONE_CONFIG])
        
        try:
            proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            while proc.poll() is None:
                status_queue.put((label, "TRANSFERRING", f"{file_path} ({size_gb} GB)"))
                time.sleep(5)
            
            if proc.returncode == 0:
                mark_large_file_complete(folder_name, file_path)
                status_queue.put((label, "COMPLETED", f"✓ {file_path}"))
            else:
                err = ""
                try:
                    err = proc.stderr.read().decode('utf-8', errors='replace')[:60]
                except Exception:
                    pass
                failed_large.append(file_path)
                status_queue.put((label, "ERROR", f"{file_path}: {err[:30]}"))
        except Exception as e:
            failed_large.append(file_path)
            status_queue.put((label, "ERROR", f"{file_path}: {str(e)[:30]}"))
    
    return failed_large

# ============ NORMAL ZIP PIPELINE ============

def pipeline_worker(task_data) -> bool:
    """
    The Core Logic for normal files (≤ threshold):
    Returns True on success, False on failure.
    """
    (original_file_list, folder_path, base_s3_key, part_name, folder_name, status_queue, lock) = task_data
    
    global _progress_lock
    _progress_lock = lock
    
    # Check dependencies
    if shutil.which("rclone") is None:
        status_queue.put((part_name, "ERROR", "Rclone Missing"))
        return False
    if shutil.which("zip") is None:
        status_queue.put((part_name, "ERROR", "Zip Missing"))
        return False
    
    s3 = get_s3_client()
    
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
            return True
    
    remaining_files = original_file_list[:]
    split_index = 0
    
    # === FIX: REMOVED EARLY EXIT CHECK ===
    # We do NOT check 'is_key_complete(base_s3_key)' here BEFORE the loop.
    # Why? Because if 'Part1.zip' is done but 'Part1_Split1.zip' failed, 
    # we need the loop to run to handle the split logic.
    # Checking here would cause IMMEDIATE EXIT, abandoning remaining_files.
    # We only skip specific keys *inside* the loop where we check each split individually.
    
    # === SMART LOOP ===
    while len(remaining_files) > 0:
        # Determine current key name
        if split_index == 0:
            current_s3_key = base_s3_key
            current_status_name = part_name
        else:
            ext = base_s3_key.split('.')[-1]
            base = base_s3_key.replace(f".{ext}", "")
            current_s3_key = f"{base}_Split{split_index}.{ext}"
            current_status_name = f"{part_name}.{split_index}"
        
        # Check progress JSON for THIS specific key (Source of Truth)
        if is_key_complete(folder_name, current_s3_key):
            status_queue.put((current_status_name, "SKIPPED", "Split done (JSON)"))
            split_index += 1
            continue
        
        temp_dir = os.path.join(WORK_DIR, f"temp_{part_name}_{split_index}_{random.randint(1000,9999)}")
        zip_filename = current_s3_key.split('/')[-1]
        local_zip = os.path.join(WORK_DIR, zip_filename)
        proc = None
        disk_triggered = False
        size_triggered = False
        
        try:
            os.makedirs(temp_dir, exist_ok=True)
            
            list_path = os.path.join(temp_dir, "filelist.txt")
            with open(list_path, 'w', encoding='utf-8') as f:
                for item in remaining_files:
                    f.write(f"{item}\n")
            
            status_queue.put((current_status_name, "DOWNLOADING", f"Target: {len(remaining_files)} files"))
            
            cmd_dl = ['rclone', 'copy', folder_path, temp_dir, '--files-from', list_path,
                      f'--transfers={DOWNLOAD_THREADS}',
                      '--ignore-errors', '--no-traverse', '--quiet']
            
            if os.path.exists(RCLONE_CONFIG):
                cmd_dl.extend(['--config', RCLONE_CONFIG])
            
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
                time.sleep(2)  # Let things settle
            
            # === INVENTORY CHECK ===
            downloaded_files = []
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if file == "filelist.txt":
                        continue
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
                
                if os.path.exists(list_path):
                    try:
                        os.remove(list_path)
                    except OSError:
                        pass
                
                cmd_zip = ["zip", "-0", "-r", "-q", local_zip, "."]
                result = subprocess.run(cmd_zip, cwd=temp_dir, capture_output=True)
                
                if os.path.exists(local_zip):
                    file_size = os.path.getsize(local_zip)
                    status_queue.put((current_status_name, "UPLOADING", f"{int(file_size/(1024*1024))} MB"))
                    s3.upload_file(local_zip, S3_BUCKET, current_s3_key)
                    mark_part_complete(folder_name, current_s3_key, downloaded_files)
                    status_queue.put((current_status_name, "COMPLETED", "Saved to S3 ✓"))
                else:
                    raise Exception(f"Zip file {zip_filename} not created")
            else:
                if not disk_triggered and not size_triggered and proc.returncode != 0:
                    err_msg = "Rclone Failed"
                    if proc.stderr:
                        try:
                            err_output = proc.stderr.read().decode('utf-8', errors='replace')[:200]
                            if err_output.strip():
                                err_msg = f"Rclone: {err_output.strip()[:40]}"
                        except Exception:
                            pass
                    status_queue.put((current_status_name, "ERROR", err_msg))
                    return False
            
        except Exception as e:
            status_queue.put((current_status_name, "ERROR", str(e)[:40]))
            return False
        
        finally:
            # Cleanup
            if proc and proc.poll() is None:
                try:
                    proc.kill()
                except Exception:
                    pass
            
            if os.path.exists(local_zip):
                try:
                    os.remove(local_zip)
                except OSError:
                    pass
            
            if os.path.exists(temp_dir):
                try:
                    shutil.rmtree(temp_dir, onerror=handle_remove_readonly)
                except Exception:
                    subprocess.run(["rm", "-rf", temp_dir], capture_output=True)
        
        if len(remaining_files) > 0:
            split_index += 1
            trigger = "size cap" if size_triggered else "disk"
            status_queue.put((part_name, "SPLITTING", f"{len(remaining_files)} remain ({trigger})"))
        else:
            break
    
    return True

# ============ MONITOR ============

def monitor(queue, num_parts: int) -> None:
    """Live status monitor. Stops on sentinel (None, ...)."""
    statuses = {}
    
    # Check if we have a TTY for colors
    has_color = sys.stdout.isatty()
    
    def colorize(text: str, code: str) -> str:
        return f"\033[{code}m{text}\033[0m" if has_color else text
    
    print("\n" * (MAX_PARALLEL_WORKERS + 5))
    
    while True:
        try:
            while not queue.empty():
                part, state, info = queue.get()
                if part is None:  # Sentinel: stop monitor
                    return
                statuses[part] = (state, info)
        except Exception:
            pass
        
        # Move cursor up
        if has_color:
            sys.stdout.write(f"\033[{len(statuses)+5}A")
        
        print(f"{'PART':<20} | {'STATUS':<15} | {'INFO':<30}")
        print("-" * 70)
        
        done = 0
        
        def natural_sort_key(s):
            return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]
        
        sorted_keys = sorted(statuses.keys(), key=natural_sort_key)
        
        for p in sorted_keys:
            state, info = statuses[p]
            if state in ["COMPLETED", "SKIPPED", "ERROR"]:
                done += 1
            
            row = f"{p:<20} | {state:<15} | {info:<30}"
            
            if has_color:
                if state == "ERROR":
                    row = colorize(row, "91")  # Red
                elif state in ["COMPLETED", "SKIPPED"]:
                    row = colorize(row, "92")  # Green
                elif state == "RESUMED":
                    row = colorize(row, "96")  # Cyan
                elif state in ["DIRECT COPY", "TRANSFERRING"]:
                    row = colorize(row, "95")  # Magenta
                elif "DISK FULL" in state or "SIZE CAP" in state:
                    row = colorize(row, "93")  # Yellow
            
            print(row)
        
        sys.stdout.flush()
        time.sleep(1)

# ============ MAIN ============

def main():
    print("🚀 PYTHON MASTER WORKER (Fixed Version)")
    print("=" * 60)
    print(f"   Source       : {SOURCE}")
    print(f"   Destination  : {DESTINATION} (large files only)")
    print(f"   S3 Bucket    : {S3_BUCKET}")
    print(f"   Max Zip Size : {MAX_ZIP_SIZE_GB} GB")
    print(f"   Work Dir     : {WORK_DIR}")
    print("=" * 60)
    
    # Check credentials
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        print("\n❌ AWS credentials not configured!")
        print("   Set environment variables:")
        print("   export AWS_ACCESS_KEY_ID='your_key'")
        print("   export AWS_SECRET_ACCESS_KEY='your_secret'")
        return
    
    print("\n🛠️  Checking dependencies...")
    
    # Install zip if needed
    try:
        subprocess.run("apt-get update && apt-get install -y zip", 
                      shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                      timeout=60)
    except Exception:
        pass  # May not be on Ubuntu/Debian
    
    if shutil.which("rclone") is None:
        print("   ⬇️  Installing Rclone...")
        try:
            subprocess.run("curl https://rclone.org/install.sh | sudo bash", 
                          shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                          timeout=120)
        except Exception as e:
            print(f"   ❌ Failed to install rclone: {e}")
            return
    
    if shutil.which("zip") is None:
        print("   ⚠️  zip command not found, installing...")
    
    print("✅ Dependencies ready!\n")
    
    # Test S3 connection
    print("🔌 Testing S3 connection...")
    try:
        s3 = get_s3_client()
        s3.head_bucket(Bucket=S3_BUCKET)
        print("   ✅ S3 connection successful\n")
    except Exception as e:
        print(f"   ❌ S3 connection failed: {e}\n")
        return
    
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
        lock = m.Lock()
        
        # Start monitor
        total_parts = math.ceil(len(files) / SPLIT_THRESHOLD) if files else 0
        monitor_thread = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        monitor_thread.submit(monitor, q, total_parts + (1 if remaining_large else 0))
        
        # Run tasks
        has_failures = False
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS + 1) as thread_exe:
            futures = []
            large_future = None
            
            # Submit large file transfer
            if remaining_large:
                print(f"   ⚡ {len(remaining_large)} large file(s) → direct transfer to {DESTINATION}")
                large_future = thread_exe.submit(transfer_large_files, folder, q, lock)
                futures.append(large_future)
            
            # Submit normal zip pipeline
            if files:
                num_parts = math.ceil(len(files) / SPLIT_THRESHOLD)
                print(f"   🔹 {len(files)} normal files → {num_parts} part(s)")
                
                def run_zip_pipeline():
                    tasks = []
                    for i in range(num_parts):
                        batch = files[i*SPLIT_THRESHOLD:(i+1)*SPLIT_THRESHOLD]
                        part = f"Part{i+1}" if num_parts > 1 else "Full"
                        s3_key = f"{S3_PREFIX}{sanitize_name(folder)}_{part}.zip"
                        tasks.append((batch, f"{SOURCE}/{folder}", s3_key, part, folder, q, lock))
                    
                    with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as exe:
                        results = list(exe.map(pipeline_worker, tasks))
                    # Return True if ANY worker failed
                    return any(r is False for r in results)
                
                futures.append(thread_exe.submit(run_zip_pipeline))
            
            # Wait for all
            for f in futures:
                try:
                    result = f.result()
                    # run_zip_pipeline returns True if any worker failed
                    if result is True and f != large_future:
                        has_failures = True
                        print(f"   ❌ Some zip pipeline worker(s) FAILED!")
                except Exception as e:
                    has_failures = True
                    print(f"   ❌ Future failed: {e}")
            
            # Check large file transfer results
            if large_future and large_future.done():
                try:
                    failed_large_files = large_future.result()
                    if failed_large_files:
                        has_failures = True
                        print(f"   ❌ {len(failed_large_files)} large file(s) FAILED!")
                except Exception:
                    has_failures = True
        
        # Mark complete or not
        if has_failures:
            print(f"\n⚠️  {folder} — INCOMPLETE (some transfers failed, will retry on next run)\n")
        else:
            mark_folder_complete(folder)
            print(f"\n✅ {folder} — ALL DONE\n")
        
        # Stop monitor
        q.put((None, "DONE", ""))
    
    print("\n🏁 ALL FOLDERS COMPLETE!")


if __name__ == "__main__":
    main()
