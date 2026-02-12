#!/usr/bin/env python3
"""
PYTHON UNZIPPER & MERGER (v3 - Fully Fixed + All v2 Bugs Resolved)
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

ALL V1 BUGS FIXED:
- Environment variables for credentials (no hardcoding)
- Null-safe lock handling
- Proper exception handling (no bare except)
- Safe subprocess calls (no shell injection)
- Safe S3 key encoding
- Configurable paths (not hardcoded to Colab)
- Division by zero protection
- Accurate zip bomb detection (no integer truncation)
- Proper error messages
- boto3 exception handling (all functions)
- Race condition fix (per-folder progress files)
- S3 operation timeouts

V2 → V3 ADDITIONAL FIXES:
- Replaced shell=True for rclone install (security)
- Added timeout to queue.get() in monitor (prevents hang)
- Added retry logic for transient S3 failures (3 retries)
- Fixed thread safety issue with status_queue
- Added validation for S3 download success
- Added orphaned temp directory cleanup on startup
- Added signal handling for graceful shutdown
- Consistent progress file naming with zipper
- Removed unused math import
- Better error handling for unzip failures
"""

import subprocess
import sys
import time
import os
import shutil
import stat
import random
import re
import json
import signal
import concurrent.futures
import multiprocessing
import threading
from urllib.parse import quote
from typing import Optional, Set, List, Dict, Any

# Check boto3 early
try:
    import boto3
    import botocore.exceptions
    from botocore.config import Config
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
S3_MAX_RETRIES = 3          # Max retries for transient S3 failures

# Paths - configurable via environment
WORK_DIR = os.environ.get("WORK_DIR", "/content")
RCLONE_CONFIG = os.environ.get("RCLONE_CONFIG", "/content/rclone.conf")
LOCAL_OUTPUT_DIR = os.environ.get("LOCAL_OUTPUT_DIR", "/content/merged_output")

# =======================================

# Process-safe lock — will be set in worker processes
_progress_lock: Optional[Any] = None
_stop_monitor = threading.Event()
_shutdown_requested = threading.Event()

# Zip bomb detection: max allowed ratio of extracted size to zip file size
MAX_ZIP_BOMB_RATIO = 100  # extracted can be at most 100x the zip size

# ============ S3 CONFIG WITH TIMEOUTS ============
S3_CONFIG = Config(
    connect_timeout=30,
    read_timeout=300,  # 5 minutes for large downloads
    retries={'max_attempts': 3}
)

# ============ S3 FOLDER INDEX ============
FOLDER_INDEX_KEY = f"{S3_PREFIX}_index/folder_list.txt"


# ============ S3 PROGRESS TRACKING ============
# V3 FIX: Consistent naming with zipper

def get_progress_key(folder_name: str) -> str:
    """Get per-folder progress file key to avoid race conditions."""
    safe_name = sanitize_name(folder_name)
    return f"{S3_PREFIX}_progress/{safe_name}_unzip_progress.json"


def sanitize_name(name: str) -> str:
    """Sanitize name for S3 key while preserving readability."""
    return quote(name, safe='').replace('%20', '_').replace('%2F', '_')


def get_s3_client():
    """Create S3 client with validation and timeouts."""
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
        endpoint_url=S3_ENDPOINT,
        config=S3_CONFIG
    )


def s3_operation_with_retry(operation_func, max_retries: int = S3_MAX_RETRIES) -> Any:
    """
    Execute S3 operation with retry logic for transient failures.
    Returns the result or raises the last exception.
    """
    last_exception = None
    for attempt in range(max_retries):
        try:
            return operation_func()
        except botocore.exceptions.ConnectionError as e:
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"   ⚠️ S3 connection error, retrying in {wait_time}s... ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ('NoSuchKey', 'AccessDenied', 'InvalidAccessKeyId'):
                raise
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"   ⚠️ S3 client error, retrying in {wait_time}s... ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"   ⚠️ S3 error, retrying in {wait_time}s... ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
    
    raise last_exception if last_exception else Exception("Unknown S3 error")


def fetch_folder_list() -> List[str]:
    """Fetch the folder list from S3 (created by mapper.py)."""
    def _fetch():
        s3 = get_s3_client()
        response = s3.get_object(Bucket=S3_BUCKET, Key=FOLDER_INDEX_KEY)
        content = response['Body'].read().decode('utf-8')
        return [line.strip() for line in content.splitlines() if line.strip()]
    
    try:
        folders = s3_operation_with_retry(_fetch)
        print(f"   📁 Found {len(folders)} folders from S3 index")
        return folders
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code in ('NoSuchKey', '404'):
            print(f"   ❌ Folder index not found on S3")
        else:
            print(f"   ❌ Could not fetch folder index from S3: {e}")
        print(f"   💡 Run mapper.py first to create the folder index.")
        return []
    except Exception as e:
        print(f"   ❌ Unexpected error fetching folder index: {e}")
        return []


def load_progress(folder_name: str) -> Dict[str, Any]:
    """Load progress JSON from S3 for a specific folder. Returns dict or empty dict on failure."""
    progress_key = get_progress_key(folder_name)
    
    def _load():
        s3 = get_s3_client()
        response = s3.get_object(Bucket=S3_BUCKET, Key=progress_key)
        return json.loads(response['Body'].read().decode('utf-8'))
    
    try:
        return s3_operation_with_retry(_load)
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code in ('NoSuchKey', '404'):
            return {}
        print(f"   ⚠️ Error loading progress from S3: {e}")
        return {}
    except json.JSONDecodeError as e:
        print(f"   ⚠️ Progress file corrupted, starting fresh: {e}")
        return {}
    except Exception as e:
        error_str = str(e)
        if 'NoSuchKey' in error_str or 'Not Found' in error_str or '404' in error_str:
            return {}
        print(f"   ⚠️ Error loading progress from S3: {e}")
        return {}


def save_progress(folder_name: str, progress: Dict[str, Any]) -> bool:
    """Save progress JSON to S3 for a specific folder. Returns True on success."""
    progress_key = get_progress_key(folder_name)
    
    def _save():
        s3 = get_s3_client()
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=progress_key,
            Body=json.dumps(progress, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        return True
    
    try:
        return s3_operation_with_retry(_save)
    except botocore.exceptions.ClientError as e:
        print(f"⚠️  Failed to save progress to S3: {e}")
        return False
    except Exception as e:
        print(f"⚠️  Unexpected error saving progress: {e}")
        return False


def _update_progress_safe(folder_name: str, update_func) -> bool:
    """Safely update progress with lock handling. Returns True on success."""
    global _progress_lock
    
    def _do_update():
        progress = load_progress(folder_name)
        update_func(progress)
        return save_progress(folder_name, progress)
    
    if _progress_lock is not None:
        with _progress_lock:
            return _do_update()
    else:
        return _do_update()


def mark_zip_processed(folder_name: str, s3_key: str) -> bool:
    """Mark a single zip as downloaded + unzipped + uploaded. Returns True on success."""
    def update(progress: Dict[str, Any]) -> None:
        if "processed_keys" not in progress:
            progress["processed_keys"] = []
        if "folder_complete" not in progress:
            progress["folder_complete"] = False
        if s3_key not in progress["processed_keys"]:
            progress["processed_keys"].append(s3_key)
    
    return _update_progress_safe(folder_name, update)


def mark_folder_complete(folder_name: str) -> bool:
    """Mark an entire folder as fully completed. Returns True on success."""
    def update(progress: Dict[str, Any]) -> None:
        progress["folder_complete"] = True
    
    return _update_progress_safe(folder_name, update)


def is_folder_complete(folder_name: str) -> bool:
    """Check if folder was fully processed in a previous run."""
    progress = load_progress(folder_name)
    return progress.get("folder_complete", False)


def get_processed_keys(folder_name: str) -> Set[str]:
    """Get the set of S3 keys already processed for a folder."""
    progress = load_progress(folder_name)
    return set(progress.get("processed_keys", []))


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


def cleanup_orphaned_temp_dirs() -> int:
    """Clean up orphaned temp directories from previous crashed runs."""
    cleaned = 0
    try:
        for item in os.listdir(WORK_DIR):
            if item.startswith("temp_") or item.startswith("unzip_"):
                item_path = os.path.join(WORK_DIR, item)
                try:
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path, onerror=handle_remove_readonly)
                        cleaned += 1
                except Exception:
                    pass
    except OSError:
        pass
    return cleaned


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
    except botocore.exceptions.ClientError as e:
        print(f"   ⚠️ Error listing S3 objects: {e}")
        return []
    except Exception as e:
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
    zip_filename = s3_key.split('/')[-1]
    local_zip = os.path.join(WORK_DIR, f"{zip_filename}_{random.randint(1000,9999)}")
    temp_unzip_dir = os.path.join(WORK_DIR, f"unzip_{zip_filename}_{random.randint(1000,9999)}")
    
    try:
        os.makedirs(temp_unzip_dir, exist_ok=True)
        
        # 1. Download zip from S3 with retry
        status_queue.put((status_name, "DOWNLOADING", zip_filename))
        
        def _download():
            s3 = get_s3_client()
            s3.download_file(S3_BUCKET, s3_key, local_zip)
        
        try:
            s3_operation_with_retry(_download)
        except Exception as e:
            status_queue.put((status_name, "ERROR", f"S3 download failed: {str(e)[:30]}"))
            return False
        
        # V3 FIX: Verify download success
        if not os.path.exists(local_zip):
            status_queue.put((status_name, "ERROR", "Download file missing"))
            return False
        
        file_size_mb = os.path.getsize(local_zip) / (1024 * 1024)
        status_queue.put((status_name, "DOWNLOADED", f"{int(file_size_mb)} MB"))
        
        # 2. Unzip
        status_queue.put((status_name, "UNZIPPING", zip_filename))
        cmd_unzip = ["unzip", "-o", "-q", local_zip, "-d", temp_unzip_dir]
        result = subprocess.run(cmd_unzip, capture_output=True, text=True)
        
        # V3 FIX: Better unzip error handling
        if result.returncode not in (0, 1):  # 1 = warnings (OK)
            err_msg = f"unzip failed rc={result.returncode}"
            if result.stderr:
                err_msg = f"unzip: {result.stderr[:30]}"
            status_queue.put((status_name, "ERROR", err_msg))
            return False
        
        # Delete zip immediately to free disk
        if os.path.exists(local_zip):
            try:
                os.remove(local_zip)
            except OSError:
                pass
        
        # Count files and check for zip bomb
        total_files = sum(len(files) for _, _, files in os.walk(temp_unzip_dir))
        total_size_mb = get_folder_size_mb(temp_unzip_dir)
        
        # Zip bomb detection with division by zero protection
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
            
            try:
                subprocess.run(
                    ["cp", "-r", "-n", f"{temp_unzip_dir}/.", final_dir + "/"],
                    check=False,
                    capture_output=True
                )
            except Exception:
                merge_folders_safe(temp_unzip_dir, final_dir)
            
            status_queue.put((status_name, "SAVED", f"Local: {final_dir}"))
        else:
            # V3 FIX: Check rclone availability
            if shutil.which("rclone") is None:
                status_queue.put((status_name, "ERROR", "Rclone not installed"))
                return False
            
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
                # V3 FIX: Check for shutdown
                if _shutdown_requested.is_set():
                    proc.kill()
                    status_queue.put((status_name, "ABORTED", "Shutdown requested"))
                    return False
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
        if mark_zip_processed(folder_name, s3_key):
            status_queue.put((status_name, "COMPLETED", "Progress saved ✓"))
        else:
            status_queue.put((status_name, "WARN", "Upload OK, progress save failed"))
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
        
        # 3. Process each zip one at a time
        failed_zips = []
        for i, s3_key in enumerate(remaining_keys):
            # V3 FIX: Check for shutdown
            if _shutdown_requested.is_set():
                status_queue.put((folder_name, "ABORTED", "Shutdown requested"))
                return
            
            part_label = f"{folder_name}[{i+1}/{len(remaining_keys)}]"
            
            # Check disk before processing
            if check_disk_usage():
                status_queue.put((folder_name, "DISK WARN", "High disk, cleaning..."))
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


def monitor(queue, total_folders: int, stop_event: threading.Event) -> None:
    """Live status monitor with color-coded output. Stops on sentinel or stop_event."""
    statuses = {}
    
    has_color = sys.stdout.isatty()
    
    def colorize(text: str, code: str) -> str:
        return f"\033[{code}m{text}\033[0m" if has_color else text
    
    print("\n" * (total_folders + 5))
    
    while not stop_event.is_set():
        try:
            # V3 FIX: Use timeout on queue.get() to prevent infinite blocking
            while not queue.empty():
                try:
                    part, state, info = queue.get(timeout=0.1)
                    if part is None:
                        return
                    statuses[part] = (state, info)
                except Exception:
                    pass
        except Exception:
            pass
        
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
            if state in ["COMPLETED", "SKIPPED", "ERROR", "ABORTED"]:
                done += 1
            
            row = f"{p:<30} | {state:<15} | {info:<35}"
            
            if has_color:
                if state == "ERROR" or state == "ABORTED":
                    row = colorize(row, "91")
                elif state in ["COMPLETED", "SKIPPED"]:
                    row = colorize(row, "92")
                elif state == "RESUMED":
                    row = colorize(row, "96")
                elif "DISK" in state:
                    row = colorize(row, "93")
                elif state in ["UPLOADING", "UPLOADED"]:
                    row = colorize(row, "96")
            
            print(row)
        
        sys.stdout.flush()
        time.sleep(1)


# ============ SIGNAL HANDLERS ============

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    print(f"\n⚠️ Received signal {signum}, shutting down gracefully...")
    _shutdown_requested.set()
    _stop_monitor.set()


def main():
    print("📦 PYTHON UNZIPPER & MERGER (v3 - Fully Fixed)")
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
    
    # V3 FIX: Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Check credentials
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        print("\n❌ AWS credentials not configured!")
        print("   Set environment variables:")
        print("   export AWS_ACCESS_KEY_ID='your_key'")
        print("   export AWS_SECRET_ACCESS_KEY='your_secret'")
        return
    
    # 1. Install dependencies
    print("\n🛠️  Checking dependencies...")
    
    # V3 FIX: Don't use shell=True with command strings
    try:
        subprocess.run(
            ["apt-get", "update"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=60
        )
        subprocess.run(
            ["apt-get", "install", "-y", "unzip"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=60
        )
    except Exception:
        pass  # May not be on Ubuntu/Debian
    
    if shutil.which("rclone") is None and not SKIP_UPLOAD:
        print("   ⬇️  Installing Rclone...")
        try:
            # V3 FIX: More secure rclone install
            install_script = "/tmp/rclone_install.sh"
            subprocess.run(
                ["curl", "-o", install_script, "https://rclone.org/install.sh"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=60, check=True
            )
            subprocess.run(
                ["sudo", "bash", install_script],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=120
            )
            os.remove(install_script)
        except Exception as e:
            print(f"   ❌ Failed to install rclone: {e}")
            return
    
    if not SKIP_UPLOAD and not os.path.exists(RCLONE_CONFIG):
        print(f"⚠️  WARNING: {RCLONE_CONFIG} not found!")
        print("   Please upload your rclone.conf or configure rclone first.")
        print("   Or set SKIP_UPLOAD = True to only extract locally.\n")
    
    print("✅ Dependencies ready!\n")
    
    # V3 FIX: Clean up orphaned temp directories
    print("🧹 Cleaning up orphaned temp directories...")
    cleaned_temps = cleanup_orphaned_temp_dirs()
    if cleaned_temps > 0:
        print(f"   Removed {cleaned_temps} orphaned temp directories")
    
    # Test S3 connection
    print("🔌 Testing S3 connection...")
    try:
        s3 = get_s3_client()
        s3.head_bucket(Bucket=S3_BUCKET)
        print("   ✅ S3 connection successful\n")
    except Exception as e:
        print(f"   ❌ S3 connection failed: {e}\n")
        return
    
    # 2. Fetch folder list from S3
    print("📁 Fetching folder list from S3...")
    SUBFOLDERS = fetch_folder_list()
    if not SUBFOLDERS:
        print("❌ No folders to process. Run mapper.py first!")
        return
    print()
    
    # 3. Show progress summary
    print("📋 Checking progress from S3...")
    folders_with_progress = 0
    for folder in SUBFOLDERS:
        progress = load_progress(folder)
        if progress:
            folders_with_progress += 1
            done_keys = len(progress.get("processed_keys", []))
            is_done = progress.get("folder_complete", False)
            status = "✅ COMPLETE" if is_done else f"⏳ {done_keys} zips processed"
            print(f"   {folder}: {status}")
    if folders_with_progress == 0:
        print("   No previous progress found. Starting fresh.")
    print()
    
    # 4. Process each subfolder
    m = multiprocessing.Manager()
    q = m.Queue()
    lock = m.Lock()
    stop_event = threading.Event()
    
    tasks = [(folder, q, lock) for folder in SUBFOLDERS]
    
    # Start monitor
    monitor_thread = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    monitor_future = monitor_thread.submit(monitor, q, len(SUBFOLDERS), stop_event)
    
    # Process folders in parallel
    try:
        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as exe:
            exe.map(process_folder, tasks)
    except Exception as e:
        print(f"❌ Error in parallel processing: {e}")
    
    # Stop the monitor thread cleanly
    stop_event.set()
    q.put((None, "DONE", ""))
    
    print("\n\n🏁 ALL DONE!")
    if SKIP_UPLOAD:
        print(f"   Files extracted to: {LOCAL_OUTPUT_DIR}")
    else:
        print(f"   Files uploaded to: {DESTINATION}")


if __name__ == "__main__":
    main()
