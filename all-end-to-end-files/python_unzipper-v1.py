#!/usr/bin/env python3
"""
PYTHON UNZIPPER & MERGER (Fixed Version + S3 Resume)
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
- Zip bomb detection for security

FIXES APPLIED:
- Environment variables for credentials (no hardcoding)
- Null-safe lock handling
- Proper exception handling (no bare except)
- Safe subprocess calls (no shell injection)
- Safe S3 key encoding
- Configurable paths (not hardcoded to Colab)
- Division by zero protection
- Accurate zip bomb detection (no integer truncation)
- Proper error messages
- **BUGFIX**: Added missing 'json' import
- **BUGFIX**: Fixed cp command flags for local merging
- **BUGFIX**: Removed deprecated distutils dependency
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

# Target remote to upload the merged/unzipped files
DESTINATION = "gdrive:Work Files"    # rclone remote:path (destination to upload to)

# S3 / Wasabi config — must match your python_zipper.py config
S3_BUCKET = os.environ.get("S3_BUCKET", "workfiles123")
S3_PREFIX = os.environ.get("S3_PREFIX", "work_files_zips/")

# Get credentials from environment variables (SECURE - never hardcode!)
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "https://s3.ap-northeast-1.wasabisys.com")

# Tuning
MAX_PARALLEL_WORKERS = 2    # Number of simultaneous folders to process
UPLOAD_THREADS = 6          # Rclone transfers per worker for upload
DISK_LIMIT_PERCENT = 80     # Trigger cleanup at this disk usage %
SKIP_UPLOAD = False         # Set True to only unzip locally without uploading

# Paths - configurable via environment
WORK_DIR = os.environ.get("WORK_DIR", "/content")
RCLONE_CONFIG = os.environ.get("RCLONE_CONFIG", "/content/rclone.conf")
LOCAL_OUTPUT_DIR = os.environ.get("LOCAL_OUTPUT_DIR", "/content/merged_output")

# =======================================

# Process-safe lock — will be set in worker processes
_progress_lock: Optional[multiprocessing.Manager().Lock] = None

# Zip bomb detection: max allowed ratio of extracted size to zip file size
MAX_ZIP_BOMB_RATIO = 100  # extracted can be at most 100x the zip size

# ============ S3 FOLDER INDEX ============
FOLDER_INDEX_KEY = f"{S3_PREFIX}_index/folder_list.txt"

# ============ S3 PROGRESS TRACKING ============
PROGRESS_KEY = f"{S3_PREFIX}_progress/unzipper_progress.json"


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


def mark_zip_processed(folder_name: str, s3_key: str) -> None:
    """Mark a single zip as downloaded + unzipped + uploaded."""
    def update():
        progress = load_progress()
        if folder_name not in progress:
            progress[folder_name] = {"processed_keys": [], "folder_complete": False}
        if s3_key not in progress[folder_name]["processed_keys"]:
            progress[folder_name]["processed_keys"].append(s3_key)
        save_progress(progress)
    
    _update_progress_safe(update)


def mark_folder_complete(folder_name: str) -> None:
    """Mark an entire folder as fully completed."""
    def update():
        progress = load_progress()
        if folder_name not in progress:
            progress[folder_name] = {"processed_keys": [], "folder_complete": False}
        progress[folder_name]["folder_complete"] = True
        save_progress(progress)
    
    _update_progress_safe(update)


def is_folder_complete(folder_name: str) -> bool:
    """Check if folder was fully processed in a previous run."""
    progress = load_progress()
    return progress.get(folder_name, {}).get("folder_complete", False)


def get_processed_keys(folder_name: str) -> Set[str]:
    """Get the set of S3 keys already processed for a folder."""
    progress = load_progress()
    return set(progress.get(folder_name, {}).get("processed_keys", []))


# ============ UTILITY FUNCTIONS ============

def get_folder_size_mb(path: str) -> float:
    """Calculates folder size in MB. Returns 0.0 on error."""
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
    return total_size / (1024 * 1024)


def check_disk_usage() -> bool:
    """Returns True if disk usage exceeds DISK_LIMIT_PERCENT."""
    try:
        total, used, free = shutil.disk_usage("/")
        if total > 0:
            percent = (used / total) * 100
            return percent > DISK_LIMIT_PERCENT
    except (OSError, IOError):
        pass
    return False


def handle_remove_readonly(func, path: str, exc) -> None:
    """Force deletes read-only files."""
    excvalue = exc[1]
    if func in (os.rmdir, os.remove, os.unlink) and hasattr(excvalue, 'errno') and excvalue.errno == 13:
        try:
            os.chmod(path, stat.S_IWRITE)
            func(path)
        except (OSError, IOError):
            raise
    else:
        raise


def list_s3_zips_for_folder(folder_name: str) -> List[str]:
    """
    Lists all zip files on S3 for a given folder.
    Returns them sorted so splits are in order.
    """
    s3 = get_s3_client()
    safe_name = sanitize_name(folder_name)
    prefix = f"{S3_PREFIX}{safe_name}_"
    
    all_keys = []
    try:
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.zip'):
                    all_keys.append(key)
    except boto3.exceptions.Boto3Error as e:
        print(f"   ⚠️ Error listing S3 objects: {e}")
        return []
    
    def natural_sort_key(s):
        return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]
    
    all_keys.sort(key=natural_sort_key)
    return all_keys


def merge_folders_safe(src: str, dst: str) -> None:
    """
    Recursively merge src directory into dst directory.
    Does not overwrite existing files in dst.
    """
    if not os.path.exists(dst):
        os.makedirs(dst)
    
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        
        if os.path.isdir(s):
            merge_folders_safe(s, d)
        else:
            if not os.path.exists(d):
                try:
                    shutil.copy2(s, d)
                except OSError:
                    pass


def download_unzip_upload_one(s3_key: str, folder_name: str, status_name: str, 
                               status_queue) -> bool:
    """
    Downloads a single zip from S3, unzips it into a temp dir,
    uploads it to the target remote via rclone, then cleans up.
    Returns True on success.
    """
    s3 = get_s3_client()
    zip_filename = s3_key.split('/')[-1]
    local_zip = os.path.join(WORK_DIR, f"{zip_filename}_{random.randint(1000,9999)}")
    temp_unzip_dir = os.path.join(WORK_DIR, f"unzip_{zip_filename}_{random.randint(1000,9999)}")
    
    try:
        os.makedirs(temp_unzip_dir, exist_ok=True)
        
        # 1. Download zip from S3
        status_queue.put((status_name, "DOWNLOADING", zip_filename))
        try:
            s3.download_file(S3_BUCKET, s3_key, local_zip)
        except boto3.exceptions.Boto3Error as e:
            status_queue.put((status_name, "ERROR", f"S3 download failed: {str(e)[:30]}"))
            return False
        
        file_size_mb = os.path.getsize(local_zip) / (1024 * 1024)
        status_queue.put((status_name, "DOWNLOADED", f"{int(file_size_mb)} MB"))
        
        # 2. Unzip
        status_queue.put((status_name, "UNZIPPING", zip_filename))
        cmd_unzip = ["unzip", "-o", "-q", local_zip, "-d", temp_unzip_dir]
        result = subprocess.run(cmd_unzip, capture_output=True, text=True)
        
        if result.returncode not in (0, 1):  # 1 = warnings (OK)
            status_queue.put((status_name, "ERROR", f"unzip failed rc={result.returncode}"))
            return False
        
        # Delete zip immediately to free disk
        if os.path.exists(local_zip):
            try:
                os.remove(local_zip)
            except OSError:
                pass
        
        # Count files and check for zip bomb
        total_files = sum(len(files) for _, _, files in os.walk(temp_unzip_dir))
        total_size_mb = get_folder_size_mb(temp_unzip_dir)  # Keep as float!
        
        # Zip bomb detection: if extracted is >100x the zip size, abort
        if file_size_mb > 0 and (total_size_mb / file_size_mb) > MAX_ZIP_BOMB_RATIO:
            status_queue.put((status_name, "ERROR", 
                f"Zip bomb detected! Ratio: {int(total_size_mb / file_size_mb)}x"))
            return False
        
        status_queue.put((status_name, "UNZIPPED", 
            f"{total_files} files, {int(total_size_mb)} MB"))
        
        # 3. Upload or move locally
        if SKIP_UPLOAD:
            final_dir = os.path.join(LOCAL_OUTPUT_DIR, folder_name)
            os.makedirs(final_dir, exist_ok=True)
            
            # FIX: Use 'cp -rn' (recursive, no clobber) for Linux/Mac
            # Removed --no-target-directory which broke directory merging
            try:
                subprocess.run(
                    ["cp", "-r", "-n", f"{temp_unzip_dir}/.", final_dir + "/"],
                    check=False,
                    capture_output=True
                )
            except Exception:
                # Fallback: Pure Python recursive merge
                merge_folders_safe(temp_unzip_dir, final_dir)
            
            status_queue.put((status_name, "SAVED", f"Local: {final_dir}"))
        else:
            target = f"{DESTINATION}/{folder_name}"
            status_queue.put((status_name, "UPLOADING", f"→ {target}"))
            
            cmd_upload = [
                'rclone', 'copy',
                temp_unzip_dir, target,
                f'--transfers={UPLOAD_THREADS}',
                '--ignore-errors',
                '--quiet'
            ]
            
            if os.path.exists(RCLONE_CONFIG):
                cmd_upload.extend(['--config', RCLONE_CONFIG])
            
            proc = subprocess.Popen(cmd_upload, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            while proc.poll() is None:
                status_queue.put((status_name, "UPLOADING", f"{total_files} files → remote"))
                time.sleep(3)
            
            if proc.returncode != 0:
                err = ""
                try:
                    err = proc.stderr.read().decode('utf-8', errors='replace')[:100]
                except Exception:
                    pass
                status_queue.put((status_name, "UPLOAD_ERR", err[:40] if err else "Unknown error"))
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
            except OSError:
                pass
        if os.path.exists(temp_unzip_dir):
            try:
                shutil.rmtree(temp_unzip_dir, onerror=handle_remove_readonly)
            except Exception:
                subprocess.run(["rm", "-rf", temp_unzip_dir], capture_output=True)


def process_folder(args) -> None:
    """
    Main worker for one subfolder:
    1. List all zip parts on S3
    2. Skip already-processed zips (resume)
    3. For each remaining zip: download → unzip → upload → cleanup → save progress
    """
    folder_name, status_queue, lock = args
    
    global _progress_lock
    _progress_lock = lock
    
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
        failed_zips = []
        for i, s3_key in enumerate(remaining_keys):
            part_label = f"{folder_name}[{i+1}/{len(remaining_keys)}]"
            
            # Check disk before processing
            if check_disk_usage():
                status_queue.put((folder_name, "DISK WARN", "High disk, cleaning..."))
                # Force cleanup of temp files
                try:
                    for item in os.listdir(WORK_DIR):
                        item_path = os.path.join(WORK_DIR, item)
                        if item.startswith("unzip_") or item.startswith("merge_"):
                            try:
                                if os.path.isdir(item_path):
                                    shutil.rmtree(item_path, onerror=handle_remove_readonly)
                            except Exception:
                                pass
                except OSError:
                    pass
            
            success = download_unzip_upload_one(s3_key, folder_name, part_label, status_queue)
            if not success:
                failed_zips.append(s3_key)
                status_queue.put((folder_name, "WARN", 
                    f"Failed: {s3_key.split('/')[-1]}, continuing..."))
        
        # 4. Only mark folder complete if ALL zips succeeded
        if failed_zips:
            status_queue.put((folder_name, "ERROR", 
                f"{len(failed_zips)} zip(s) FAILED! Not marking complete."))
        else:
            mark_folder_complete(folder_name)
            status_queue.put((folder_name, "COMPLETED", "All zips processed ✓"))
    
    except Exception as e:
        status_queue.put((folder_name, "ERROR", str(e)[:40]))


def monitor(queue, total_folders: int) -> None:
    """Live status monitor with color-coded output. Stops on sentinel."""
    statuses = {}
    
    # Check if we have a TTY for colors
    has_color = sys.stdout.isatty()
    
    def colorize(text: str, code: str) -> str:
        return f"\033[{code}m{text}\033[0m" if has_color else text
    
    print("\n" * (total_folders + 5))
    
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
        
        print(f"{'FOLDER/PART':<30} | {'STATUS':<15} | {'INFO':<35}")
        print("-" * 85)
        
        done = 0
        
        def natural_sort_key(s):
            return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]
        
        sorted_keys = sorted(statuses.keys(), key=natural_sort_key)
        
        for p in sorted_keys:
            state, info = statuses[p]
            if state in ["COMPLETED", "SKIPPED", "ERROR"]:
                done += 1
            
            row = f"{p:<30} | {state:<15} | {info:<35}"
            
            if has_color:
                if state == "ERROR":
                    row = colorize(row, "91")  # Red
                elif state in ["COMPLETED", "SKIPPED"]:
                    row = colorize(row, "92")  # Green
                elif state == "RESUMED":
                    row = colorize(row, "96")  # Cyan
                elif "DISK" in state:
                    row = colorize(row, "93")  # Yellow
                elif state in ["UPLOADING", "UPLOADED"]:
                    row = colorize(row, "96")  # Cyan
            
            print(row)
        
        sys.stdout.flush()
        time.sleep(1)


def main():
    print("📦 PYTHON UNZIPPER & MERGER (Fixed Version)")
    print("=" * 55)
    print(f"   S3 Bucket  : {S3_BUCKET}")
    print(f"   S3 Prefix  : {S3_PREFIX}")
    if SKIP_UPLOAD:
        print(f"   Output     : LOCAL → {LOCAL_OUTPUT_DIR}")
    else:
        print(f"   Target     : {DESTINATION}")
    print(f"   Workers    : {MAX_PARALLEL_WORKERS}")
    print(f"   Work Dir   : {WORK_DIR}")
    print("=" * 55)
    
    # Check credentials
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        print("\n❌ AWS credentials not configured!")
        print("   Set environment variables:")
        print("   export AWS_ACCESS_KEY_ID='your_key'")
        print("   export AWS_SECRET_ACCESS_KEY='your_secret'")
        return
    
    # 1. Install dependencies
    print("\n🛠️  Checking dependencies...")
    
    try:
        subprocess.run(
            "apt-get update && apt-get install -y unzip",
            shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=60
        )
    except Exception:
        pass  # May not be on Ubuntu/Debian
    
    if shutil.which("rclone") is None and not SKIP_UPLOAD:
        print("   ⬇️  Installing Rclone...")
        try:
            subprocess.run(
                "curl https://rclone.org/install.sh | sudo bash",
                shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=120
            )
        except Exception as e:
            print(f"   ❌ Failed to install rclone: {e}")
            return
    
    if not SKIP_UPLOAD and not os.path.exists(RCLONE_CONFIG):
        print(f"⚠️  WARNING: {RCLONE_CONFIG} not found!")
        print("   Please upload your rclone.conf or configure rclone first.")
        print("   Or set SKIP_UPLOAD = True to only extract locally.\n")
    
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
    
    # 3. Fetch folder list from S3
    print("📁 Fetching folder list from S3...")
    SUBFOLDERS = fetch_folder_list()
    if not SUBFOLDERS:
        print("❌ No folders to process. Run mapper.py first!")
        return
    print()
    
    # 4. Process each subfolder
    m = multiprocessing.Manager()
    q = m.Queue()
    lock = m.Lock()
    
    tasks = [(folder, q, lock) for folder in SUBFOLDERS]
    
    # Start monitor
    monitor_thread = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    monitor_thread.submit(monitor, q, len(SUBFOLDERS))
    
    # Process folders in parallel
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as exe:
            exe.map(process_folder, tasks)
    except Exception as e:
        print(f"❌ Error in parallel processing: {e}")
    
    # Stop the monitor thread cleanly
    q.put((None, "DONE", ""))
    
    print("\n\n🏁 ALL DONE!")
    if SKIP_UPLOAD:
        print(f"   Files extracted to: {LOCAL_OUTPUT_DIR}")
    else:
        print(f"   Files uploaded to: {DESTINATION}")


if __name__ == "__main__":
    main()
