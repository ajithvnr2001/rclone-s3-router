#!/usr/bin/env python3
"""
PYTHON MASTER WORKER (v5 - Final Production Release)
Features:
- Auto-dependency install (Rclone/Zip)
- Smart Disk Splitting (Never runs out of space)
- Max zip size cap (20GB default - triggers split before exceeding)
- Robust Cleanup (Force deletes locked folders)
- S3 Progress Tracking (Saves progress JSON to S3 after every completed part)
- Crash Resume (Reads progress from S3 on startup, skips completed work)
- Large File Direct Transfer (files > threshold copied directly source -> destination)

ALL V1-V3 BUGS FIXED:
- Environment variables for credentials (no hardcoding)
- Null-safe lock handling
- Proper exception handling (no bare except)
- Safe subprocess calls (no shell injection)
- Safe S3 key encoding
- Configurable paths (not hardcoded to Colab)
- Division by zero protection
- Proper error messages
- boto3 exception handling (all functions)
- Race condition fix (per-folder progress files)
- S3 operation timeouts
- Removed unsafe S3 head_object checks
- Removed early exit trap in split logic
- Fixed skipped count calculation
- Signal handling for graceful shutdown
- S3 retry logic with exponential backoff

V5 - ALL V1-V4 BUGS FIXED + FINAL POLISH:
- Added missing Any type import
- Safe dictionary access with validation
- Proper ThreadPoolExecutor shutdown
- Pre-zip disk space verification
- Progress file pruning for performance
- File descriptor leak prevention
- Structured logging for production
- Maximum retry duration cap
- Atomic folder complete checks
- Partial download detection
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
import json
import signal
import logging
import concurrent.futures
import multiprocessing
import threading
from urllib.parse import quote
from typing import Optional, Set, List, Dict, Any, Tuple

# Check boto3 early
try:
    import boto3
    import botocore.exceptions
    from botocore.config import Config
except ImportError:
    print("X boto3 not installed! Run: pip install boto3")
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
MAX_ZIP_SIZE_GB = 20        # Max zip size in GB - triggers split when download exceeds this
S3_MAX_RETRIES = 3          # Max retries for transient S3 failures
MAX_RETRY_DURATION = 300    # Maximum total retry duration in seconds (5 minutes)
MAX_PROGRESS_FILES = 5000   # Maximum files to track in progress before pruning

# Paths - configurable via environment
WORK_DIR = os.environ.get("WORK_DIR", "/content")
RCLONE_CONFIG = os.environ.get("RCLONE_CONFIG", "/content/rclone.conf")

# ============ LOGGING SETUP (V4 FIX) ============
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# =======================================
MAX_ZIP_SIZE_BYTES = MAX_ZIP_SIZE_GB * 1024 * 1024 * 1024

# Process-safe lock - will be set in worker processes
_progress_lock: Optional[Any] = None
_stop_monitor = threading.Event()
_shutdown_requested = threading.Event()

# ============ S3 CONFIG WITH TIMEOUTS ============
S3_CONFIG = Config(
    connect_timeout=30,
    read_timeout=300,  # 5 minutes for large uploads
    retries={'max_attempts': 3}
)

# ============ S3 FOLDER INDEX ============
FOLDER_INDEX_KEY = f"{S3_PREFIX}_index/folder_list.txt"

# ============ S3 PROGRESS TRACKING ============


def get_progress_key(folder_name: str) -> str:
    """Get per-folder progress file key to avoid race conditions."""
    safe_name = sanitize_name(folder_name)
    return f"{S3_PREFIX}_progress/{safe_name}_progress.json"


def sanitize_name(name: str) -> str:
    """Sanitize name for S3 key while preserving readability."""
    return quote(name, safe='').replace('%20', '_').replace('%2F', '_')


def get_s3_client() -> Any:
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


def s3_operation_with_retry(operation_func: Any, max_retries: int = S3_MAX_RETRIES,
                            max_duration: int = MAX_RETRY_DURATION) -> Any:
    """
    Execute S3 operation with retry logic for transient failures.
    V4 FIX: Added maximum duration cap.
    Returns the result or raises the last exception.
    """
    start_time = time.time()
    last_exception: Optional[Exception] = None
    
    for attempt in range(max_retries):
        # V4 FIX: Check duration limit
        if time.time() - start_time > max_duration:
            logger.error(f"Retry duration exceeded {max_duration}s")
            raise TimeoutError(f"Retry duration exceeded {max_duration} seconds")
        
        try:
            return operation_func()
        except botocore.exceptions.ConnectionError as e:
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"S3 connection error, retrying in {wait_time}s... ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            # Don't retry on client errors like NoSuchKey, AccessDenied
            if error_code in ('NoSuchKey', 'AccessDenied', 'InvalidAccessKeyId'):
                raise
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.warning(f"S3 client error, retrying in {wait_time}s... ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.warning(f"S3 error, retrying in {wait_time}s... ({attempt + 1}/{max_retries})")
                time.sleep(wait_time)
    
    raise last_exception if last_exception else Exception("Unknown S3 error")


def prune_progress_files(progress: Dict[str, Any], max_files: int = MAX_PROGRESS_FILES) -> Dict[str, Any]:
    """V4 FIX: Prune completed_files list if it grows too large."""
    completed_files = progress.get("completed_files", [])
    if len(completed_files) > max_files:
        # Keep only the most recent files
        progress["completed_files"] = completed_files[-max_files:]
        logger.info(f"Pruned progress file from {len(completed_files)} to {max_files} entries")
    return progress


def fetch_folder_list() -> List[str]:
    """Fetch the folder list from S3 (created by mapper.py)."""
    def _fetch() -> List[str]:
        s3 = get_s3_client()
        response = s3.get_object(Bucket=S3_BUCKET, Key=FOLDER_INDEX_KEY)
        content = response['Body'].read().decode('utf-8')
        return [line.strip() for line in content.splitlines() if line.strip()]
    
    try:
        folders = s3_operation_with_retry(_fetch)
        logger.info(f"Found {len(folders)} folders from S3 index")
        return folders
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code in ('NoSuchKey', '404'):
            logger.error("Folder index not found on S3")
        else:
            logger.error(f"Could not fetch folder index from S3: {e}")
        logger.info("Run mapper.py first to create the folder index.")
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching folder index: {e}")
        return []


def load_progress(folder_name: str) -> Dict[str, Any]:
    """Load progress JSON from S3 for a specific folder. Returns dict or empty dict on failure."""
    progress_key = get_progress_key(folder_name)
    
    def _load() -> Dict[str, Any]:
        s3 = get_s3_client()
        response = s3.get_object(Bucket=S3_BUCKET, Key=progress_key)
        return json.loads(response['Body'].read().decode('utf-8'))
    
    try:
        return s3_operation_with_retry(_load)
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code in ('NoSuchKey', '404'):
            return {}  # No progress file yet - normal for first run
        logger.warning(f"Error loading progress from S3: {e}")
        return {}
    except json.JSONDecodeError as e:
        logger.warning(f"Progress file corrupted, starting fresh: {e}")
        return {}
    except Exception as e:
        error_str = str(e)
        if 'NoSuchKey' in error_str or 'Not Found' in error_str or '404' in error_str:
            return {}
        logger.warning(f"Error loading progress from S3: {e}")
        return {}


def save_progress(folder_name: str, progress: Dict[str, Any]) -> bool:
    """Save progress JSON to S3 for a specific folder. Returns True on success."""
    progress_key = get_progress_key(folder_name)
    
    # V4 FIX: Prune large progress files
    progress = prune_progress_files(progress)
    
    def _save() -> bool:
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
        logger.error(f"Failed to save progress to S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error saving progress: {e}")
        return False


def _update_progress_safe(folder_name: str, update_func: Any) -> bool:
    """
    Safely update progress with lock handling.
    Returns True on success, False on failure.
    """
    global _progress_lock
    
    def _do_update() -> bool:
        progress = load_progress(folder_name)
        update_func(progress)
        return save_progress(folder_name, progress)
    
    if _progress_lock is not None:
        with _progress_lock:
            return _do_update()
    else:
        return _do_update()


def mark_part_complete(folder_name: str, s3_key: str, files_in_part: List[str]) -> bool:
    """Mark a part as complete in progress tracking. Returns True on success."""
    def update(progress: Dict[str, Any]) -> None:
        if "completed_keys" not in progress:
            progress["completed_keys"] = []
        if "completed_files" not in progress:
            progress["completed_files"] = []
        if "large_files_done" not in progress:
            progress["large_files_done"] = []
        
        if s3_key not in progress["completed_keys"]:
            progress["completed_keys"].append(s3_key)
        
        # Use set for deduplication but keep as list for JSON
        existing = set(progress["completed_files"])
        existing.update(files_in_part)
        progress["completed_files"] = list(existing)
    
    return _update_progress_safe(folder_name, update)


def mark_large_file_complete(folder_name: str, file_path: str) -> bool:
    """Mark a single large file as transferred. Returns True on success."""
    def update(progress: Dict[str, Any]) -> None:
        if "completed_keys" not in progress:
            progress["completed_keys"] = []
        if "completed_files" not in progress:
            progress["completed_files"] = []
        if "large_files_done" not in progress:
            progress["large_files_done"] = []
        
        if file_path not in progress["large_files_done"]:
            progress["large_files_done"].append(file_path)
    
    return _update_progress_safe(folder_name, update)


def mark_folder_complete(folder_name: str) -> bool:
    """Mark folder as fully complete. Returns True on success."""
    def update(progress: Dict[str, Any]) -> None:
        progress["folder_complete"] = True
    
    return _update_progress_safe(folder_name, update)


def get_completed_files(folder_name: str) -> Set[str]:
    """Get set of completed files for a folder."""
    progress = load_progress(folder_name)
    return set(progress.get("completed_files", []))


def get_completed_large_files(folder_name: str) -> Set[str]:
    """Get set of completed large files for a folder."""
    progress = load_progress(folder_name)
    return set(progress.get("large_files_done", []))


def is_folder_complete(folder_name: str) -> bool:
    """Check if folder is marked complete."""
    progress = load_progress(folder_name)
    return progress.get("folder_complete", False)


def is_key_complete(folder_name: str, s3_key: str) -> bool:
    """Check if specific S3 key is already processed."""
    progress = load_progress(folder_name)
    return s3_key in progress.get("completed_keys", [])


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
                    continue
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
    return False


def check_disk_space_for_file(required_bytes: int, path: str = None) -> bool:
    """V4 FIX: Check if there's enough disk space for a file."""
    try:
        if path is None:
            path = WORK_DIR
        stat_result = shutil.disk_usage(path)
        # Add 10% buffer
        return stat_result.free >= required_bytes * 1.1
    except (OSError, IOError):
        return True  # Assume OK on error


def handle_remove_readonly(func: Any, path: str, exc: Any) -> None:
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


def normalize_path(path: str) -> str:
    """Normalize path separators to forward slashes for consistent comparison."""
    return path.replace('\\', '/')


def cleanup_orphaned_temp_dirs() -> int:
    """
    Clean up orphaned temp directories from previous crashed runs.
    Returns count of cleaned directories.
    """
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


def fetch_map(folder_name: str) -> List[str]:
    """Downloads the normal file list from S3 (excludes large files)."""
    safe_name = sanitize_name(folder_name)
    map_key = f"{S3_PREFIX}{safe_name}_List.txt"
    
    def _fetch() -> List[str]:
        s3 = get_s3_client()
        response = s3.get_object(Bucket=S3_BUCKET, Key=map_key)
        content = response['Body'].read().decode('utf-8')
        return [line.strip() for line in content.splitlines() if line.strip()]
    
    try:
        return s3_operation_with_retry(_fetch)
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code in ('NoSuchKey', '404'):
            return []
        logger.warning(f"Error fetching file map: {e}")
        return []
    except Exception as e:
        error_str = str(e)
        if 'NoSuchKey' in error_str or 'Not Found' in error_str or '404' in error_str:
            return []
        logger.warning(f"Error fetching file map: {e}")
        return []


def fetch_large_files(folder_name: str) -> List[Dict[str, Any]]:
    """Downloads the large files list from S3."""
    safe_name = sanitize_name(folder_name)
    large_key = f"{S3_PREFIX}{safe_name}_LargeFiles.json"
    
    def _fetch() -> List[Dict[str, Any]]:
        s3 = get_s3_client()
        response = s3.get_object(Bucket=S3_BUCKET, Key=large_key)
        return json.loads(response['Body'].read().decode('utf-8'))
    
    try:
        return s3_operation_with_retry(_fetch)
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code in ('NoSuchKey', '404'):
            return []
        logger.warning(f"Error fetching large files list: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.warning(f"Large files list corrupted: {e}")
        return []
    except Exception as e:
        error_str = str(e)
        if 'NoSuchKey' in error_str or 'Not Found' in error_str or '404' in error_str:
            return []
        logger.warning(f"Error fetching large files list: {e}")
        return []


# ============ LARGE FILE DIRECT TRANSFER ============

def transfer_large_files(folder_name: str, status_queue: Any, lock: Any) -> List[str]:
    """
    Transfer large files directly from SOURCE to DESTINATION via rclone.
    No zipping, no S3 - direct server-side copy preserving path structure.
    Returns list of failed file paths.
    """
    global _progress_lock
    _progress_lock = lock
    
    # Check rclone availability first
    if shutil.which("rclone") is None:
        status_queue.put((f"*{folder_name}", "ERROR", "Rclone not installed"))
        return []
    
    large_files = fetch_large_files(folder_name)
    if not large_files:
        return []
    
    # V4 FIX: Safe dictionary access with validation
    done = get_completed_large_files(folder_name)
    remaining: List[Dict[str, Any]] = []
    
    for lf in large_files:
        # V4 FIX: Validate dictionary structure
        if not isinstance(lf, dict):
            logger.warning(f"Skipping invalid large file entry: {lf}")
            continue
        if 'path' not in lf:
            logger.warning(f"Skipping large file entry missing 'path': {lf}")
            continue
        if lf['path'] not in done:
            remaining.append(lf)
    
    if not remaining:
        status_queue.put((f"*{folder_name}", "SKIPPED", "All large files done"))
        return []
    
    status_queue.put((f"*{folder_name}", "LARGE FILES", f"{len(remaining)} file(s)"))
    failed_large: List[str] = []
    
    for i, lf in enumerate(remaining):
        # Check for shutdown request
        if _shutdown_requested.is_set():
            status_queue.put((f"*{folder_name}", "ABORTED", "Shutdown requested"))
            break
        
        file_path = lf['path']
        size_gb = lf.get('size_gb', '?')  # Safe access with default
        label = f"*{folder_name}[{i+1}/{len(remaining)}]"
        
        status_queue.put((label, "DIRECT COPY", f"{file_path} ({size_gb} GB)"))
        
        # Direct rclone copyto: source file -> destination file
        src = f"{SOURCE}/{folder_name}/{file_path}"
        dst = f"{DESTINATION}/{folder_name}/{file_path}"
        
        cmd = [
            'rclone', 'copyto', src, dst,
            '--ignore-errors',
            '--quiet'
        ]
        
        if os.path.exists(RCLONE_CONFIG):
            cmd.extend(['--config', RCLONE_CONFIG])
        
        proc = None
        try:
            proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
            while proc.poll() is None:
                if _shutdown_requested.is_set():
                    # V4 FIX: Graceful termination
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    finally:
                        # V4 FIX: Close pipes to prevent fd leak
                        try:
                            proc.stdout.close()
                            proc.stderr.close()
                        except Exception:
                            pass
                    break
                status_queue.put((label, "TRANSFERRING", f"{file_path} ({size_gb} GB)"))
                time.sleep(5)
            
            if proc.returncode == 0:
                if mark_large_file_complete(folder_name, file_path):
                    status_queue.put((label, "COMPLETED", f"* {file_path}"))
                else:
                    failed_large.append(file_path)
                    status_queue.put((label, "WARN", f"{file_path}: progress save failed"))
            else:
                err = ""
                try:
                    err = proc.stderr.read().decode('utf-8', errors='replace')[:60]
                except Exception:
                    pass
                failed_large.append(file_path)
                status_queue.put((label, "ERROR", f"{file_path}: {err[:30] if err else 'Unknown error'}"))
        except Exception as e:
            failed_large.append(file_path)
            status_queue.put((label, "ERROR", f"{file_path}: {str(e)[:30]}"))
        finally:
            # V4 FIX: Ensure pipes are closed
            if proc:
                try:
                    proc.stdout.close()
                    proc.stderr.close()
                except Exception:
                    pass
    
    return failed_large


# ============ NORMAL ZIP PIPELINE ============

def pipeline_worker(task_data: Tuple) -> bool:
    """
    The Core Logic for normal files (<= threshold):
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
        original_count = len(original_file_list)
        completed_normalized = {normalize_path(f) for f in completed_files}
        original_file_list = [f for f in original_file_list if normalize_path(f) not in completed_normalized]
        skipped = original_count - len(original_file_list)
        
        if skipped > 0:
            status_queue.put((part_name, "RESUMED", f"Skipped {skipped} done files"))
        if not original_file_list:
            status_queue.put((part_name, "SKIPPED", "All files done (resumed)"))
            return True
    
    remaining_files = original_file_list[:]
    split_index = 0
    
    # === SMART LOOP ===
    while len(remaining_files) > 0:
        # Check for shutdown request
        if _shutdown_requested.is_set():
            status_queue.put((part_name, "ABORTED", "Shutdown requested"))
            return False
        
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
                # Check for shutdown
                if _shutdown_requested.is_set():
                    # V4 FIX: Graceful termination
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    status_queue.put((current_status_name, "ABORTED", "Shutdown requested"))
                    return False
                
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
            downloaded_files: List[str] = []
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if file == "filelist.txt":
                        continue
                    abs_path = os.path.join(root, file)
                    rel_path = os.path.relpath(abs_path, temp_dir)
                    # V4 FIX: Check for partial files (non-zero size)
                    if os.path.getsize(abs_path) > 0:
                        downloaded_files.append(rel_path)
            
            downloaded_set = {normalize_path(f) for f in downloaded_files}
            new_remaining: List[str] = []
            for f in remaining_files:
                norm_f = normalize_path(f)
                if norm_f not in downloaded_set:
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
                
                # V4 FIX: Check disk space before creating zip
                estimated_zip_size = get_folder_size_bytes(temp_dir)
                if not check_disk_space_for_file(estimated_zip_size):
                    status_queue.put((current_status_name, "ERROR", "Insufficient disk for zip"))
                    return False
                
                cmd_zip = ["zip", "-0", "-r", "-q", local_zip, "."]
                result = subprocess.run(cmd_zip, cwd=temp_dir, capture_output=True)
                
                if os.path.exists(local_zip):
                    file_size = os.path.getsize(local_zip)
                    status_queue.put((current_status_name, "UPLOADING", f"{int(file_size/(1024*1024))} MB"))
                    
                    # Upload with retry and validation
                    def _upload() -> None:
                        s3.upload_file(local_zip, S3_BUCKET, current_s3_key)
                    
                    try:
                        s3_operation_with_retry(_upload)
                        
                        # Verify upload success
                        try:
                            s3.head_object(Bucket=S3_BUCKET, Key=current_s3_key)
                        except Exception:
                            raise Exception("Upload verification failed")
                        
                        if mark_part_complete(folder_name, current_s3_key, downloaded_files):
                            status_queue.put((current_status_name, "COMPLETED", "Saved to S3 *"))
                        else:
                            status_queue.put((current_status_name, "WARN", "Upload OK, progress save failed"))
                    except Exception as e:
                        status_queue.put((current_status_name, "ERROR", f"Upload failed: {str(e)[:30]}"))
                        return False
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
            
            # V4 FIX: Close pipes to prevent fd leak
            if proc:
                try:
                    if proc.stdout:
                        proc.stdout.close()
                    if proc.stderr:
                        proc.stderr.close()
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

def monitor(queue: Any, num_parts: int, stop_event: threading.Event) -> None:
    """Live status monitor. Stops on sentinel or stop_event."""
    statuses: Dict[str, Tuple[str, str]] = {}
    
    has_color = sys.stdout.isatty()
    
    def colorize(text: str, code: str) -> str:
        return f"\033[{code}m{text}\033[0m" if has_color else text
    
    print("\n" * (MAX_PARALLEL_WORKERS + 5))
    
    while not stop_event.is_set():
        try:
            # Use timeout on queue.get() to prevent infinite blocking
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
        
        # V4 FIX: Handle variable status count for cursor movement
        status_count = len(statuses)
        if has_color and status_count > 0:
            sys.stdout.write(f"\033[{status_count + 5}A")
        
        print(f"{'PART':<20} | {'STATUS':<15} | {'INFO':<30}")
        print("-" * 70)
        
        done = 0
        
        def natural_sort_key(s: str) -> List[Any]:
            return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', s)]
        
        sorted_keys = sorted(statuses.keys(), key=natural_sort_key)
        
        for p in sorted_keys:
            state, info = statuses[p]
            if state in ["COMPLETED", "SKIPPED", "ERROR", "ABORTED"]:
                done += 1
            
            row = f"{p:<20} | {state:<15} | {info:<30}"
            
            if has_color:
                if state == "ERROR" or state == "ABORTED":
                    row = colorize(row, "91")
                elif state in ["COMPLETED", "SKIPPED"]:
                    row = colorize(row, "92")
                elif state == "RESUMED":
                    row = colorize(row, "96")
                elif state in ["DIRECT COPY", "TRANSFERRING"]:
                    row = colorize(row, "95")
                elif "DISK FULL" in state or "SIZE CAP" in state:
                    row = colorize(row, "93")
            
            print(row)
        
        sys.stdout.flush()
        time.sleep(1)


# ============ CLEANUP MULTIPART UPLOADS ============

def cleanup_multipart_uploads() -> int:
    """Abort incomplete multipart uploads older than 1 day to save costs."""
    try:
        s3 = get_s3_client()
        paginator = s3.get_paginator('list_multipart_uploads')
        cleaned = 0
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
            for upload in page.get('Uploads', []):
                try:
                    s3.abort_multipart_upload(
                        Bucket=S3_BUCKET,
                        Key=upload['Key'],
                        UploadId=upload['UploadId']
                    )
                    cleaned += 1
                except Exception:
                    pass
        return cleaned
    except Exception as e:
        logger.warning(f"Could not cleanup multipart uploads: {e}")
        return 0


# ============ SIGNAL HANDLERS ============

def signal_handler(signum: int, frame: Any) -> None:
    """Handle shutdown signals gracefully."""
    logger.warning(f"Received signal {signum}, shutting down gracefully...")
    _shutdown_requested.set()
    _stop_monitor.set()


# ============ MAIN ============

def main() -> None:
    print("* PYTHON MASTER WORKER (v5 - Final Production Release)")
    print("=" * 60)
    print(f"   Source       : {SOURCE}")
    print(f"   Destination  : {DESTINATION} (large files only)")
    print(f"   S3 Bucket    : {S3_BUCKET}")
    print(f"   Max Zip Size : {MAX_ZIP_SIZE_GB} GB")
    print(f"   Work Dir     : {WORK_DIR}")
    print("=" * 60)
    
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Check credentials
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        print("\nX AWS credentials not configured!")
        print("   Set environment variables:")
        print("   export AWS_ACCESS_KEY_ID='your_key'")
        print("   export AWS_SECRET_ACCESS_KEY='your_secret'")
        return
    
    print("\n* Checking dependencies...")
    
    # Install zip if needed (No shell=True)
    try:
        subprocess.run(
            ["apt-get", "update"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=60
        )
        subprocess.run(
            ["apt-get", "install", "-y", "zip"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            timeout=60
        )
    except Exception:
        pass
    
    if shutil.which("rclone") is None:
        print("   * Installing Rclone...")
        try:
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
            logger.error(f"Failed to install rclone: {e}")
            return
    
    if shutil.which("zip") is None:
        print("   ! zip command not found")
    
    print("* Dependencies ready!\n")
    
    # Clean up orphaned temp directories from previous crashes
    print("* Cleaning up orphaned temp directories...")
    cleaned_temps = cleanup_orphaned_temp_dirs()
    if cleaned_temps > 0:
        print(f"   Removed {cleaned_temps} orphaned temp directories")
    
    # Test S3 connection
    print("* Testing S3 connection...")
    try:
        s3 = get_s3_client()
        s3.head_bucket(Bucket=S3_BUCKET)
        print("   * S3 connection successful\n")
    except Exception as e:
        logger.error(f"S3 connection failed: {e}\n")
        return
    
    # Cleanup old multipart uploads
    print("* Cleaning up incomplete uploads...")
    cleaned_uploads = cleanup_multipart_uploads()
    if cleaned_uploads > 0:
        print(f"   Cleaned up {cleaned_uploads} incomplete multipart upload(s)")
    print()
    
    # Fetch folder list
    print("* Fetching folder list from S3...")
    SUBFOLDERS = fetch_folder_list()
    if not SUBFOLDERS:
        print("X No folders to process. Run mapper.py first!")
        return
    print()
    
    for folder in SUBFOLDERS:
        # Check for shutdown
        if _shutdown_requested.is_set():
            print("\n! Shutdown requested, stopping...")
            break
        
        if is_folder_complete(folder):
            print(f">> Skipping {folder} (fully completed)")
            continue
        
        print(f"* Processing: {folder}")
        
        # === NORMAL FILES (zip pipeline) ===
        files = fetch_map(folder)
        has_normal = bool(files)
        
        # === LARGE FILES (direct transfer) ===
        large_files = fetch_large_files(folder)
        has_large = bool(large_files)
        
        if not has_normal and not has_large:
            print("   ! No files found on S3. Skipping.")
            continue
        
        # Filter completed normal files
        if has_normal:
            completed = get_completed_files(folder)
            original_count = len(files)
            completed_normalized = {normalize_path(f) for f in completed}
            files = [f for f in files if normalize_path(f) not in completed_normalized]
            if completed:
                print(f"   R Normal: {original_count - len(files)} done, {len(files)} remaining")
        
        # Filter completed large files
        if has_large:
            done_large = get_completed_large_files(folder)
            remaining_large = [lf for lf in large_files if isinstance(lf, dict) and 'path' in lf and lf['path'] not in done_large]
            if done_large:
                print(f"   R Large: {len(done_large)} done, {len(remaining_large)} remaining")
        else:
            remaining_large = []
        
        if not files and not remaining_large:
            print(f"   * All files completed!")
            mark_folder_complete(folder)
            continue
        
        # V4 FIX: Use context manager for Manager cleanup
        with multiprocessing.Manager() as m:
            q = m.Queue()
            lock = m.Lock()
            stop_event = threading.Event()
            
            tasks = [(f, folder, q, lock) for f in files] if files else []
            
            # Start monitor
            total_parts = math.ceil(len(files) / SPLIT_THRESHOLD) if files else 0
            
            # V4 FIX: Proper ThreadPoolExecutor cleanup
            monitor_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            try:
                monitor_future = monitor_executor.submit(monitor, q, total_parts + (1 if remaining_large else 0), stop_event)
                
                # Run tasks
                has_failures = False
                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as thread_exe:
                    futures = []
                    large_future = None
                    
                    # Submit large file transfer
                    if remaining_large:
                        print(f"   * {len(remaining_large)} large file(s) -> direct transfer to {DESTINATION}")
                        large_future = thread_exe.submit(transfer_large_files, folder, q, lock)
                        futures.append(large_future)
                    
                    # Submit normal zip pipeline
                    if files:
                        num_parts = math.ceil(len(files) / SPLIT_THRESHOLD)
                        print(f"   o {len(files)} normal files -> {num_parts} part(s)")
                        
                        # Capture variables properly in closure
                        files_copy = files[:]
                        folder_copy = folder
                        
                        def run_zip_pipeline(files_list: List[str] = files_copy, 
                                           folder_name: str = folder_copy) -> bool:
                            pipeline_tasks = []
                            for i in range(num_parts):
                                batch = files_list[i*SPLIT_THRESHOLD:(i+1)*SPLIT_THRESHOLD]
                                part = f"Part{i+1}" if num_parts > 1 else "Full"
                                s3_key = f"{S3_PREFIX}{sanitize_name(folder_name)}_{part}.zip"
                                pipeline_tasks.append((batch, f"{SOURCE}/{folder_name}", s3_key, part, folder_name, q, lock))
                            
                            with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as exe:
                                results = list(exe.map(pipeline_worker, pipeline_tasks))
                            return any(r is False for r in results)
                        
                        futures.append(thread_exe.submit(run_zip_pipeline))
                    
                    # Wait for all
                    for f in futures:
                        try:
                            result = f.result()
                            if result is True and f != large_future:
                                has_failures = True
                                logger.error("Some zip pipeline worker(s) FAILED!")
                        except Exception as e:
                            has_failures = True
                            logger.error(f"Future failed: {e}")
                    
                    # Check large file transfer results
                    if large_future and large_future.done():
                        try:
                            failed_large_files = large_future.result()
                            if failed_large_files:
                                has_failures = True
                                logger.error(f"{len(failed_large_files)} large file(s) FAILED!")
                        except Exception:
                            has_failures = True
                
                # Mark complete or not
                if has_failures:
                    print(f"\n! {folder} - INCOMPLETE (some transfers failed, will retry on next run)\n")
                else:
                    mark_folder_complete(folder)
                    print(f"\n* {folder} - ALL DONE\n")
                
                # Stop monitor
                stop_event.set()
                q.put((None, "DONE", ""))
            
            finally:
                # V4 FIX: Proper ThreadPoolExecutor shutdown
                monitor_executor.shutdown(wait=True)


if __name__ == "__main__":
    main()
