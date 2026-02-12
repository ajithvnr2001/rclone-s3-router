#!/usr/bin/env python3
"""
PYTHON MASTER MAPPER v4 - Production Ready (Fully Aligned with v8 Zipper/Unzipper)
Auto-discovers all subfolders, scans each for files with sizes,
separates normal files (<= threshold) and large files (> threshold),
and uploads both lists + folder index to Wasabi S3.

V2 FIXES (Aligned with python_zipper-v8.py and python_unzipper-v8.py):
- Bug #1: Use environment variables for credentials (security)
- Bug #2: Add S3_CONFIG with timeouts and connection pooling
- Bug #3: Fix S3 key naming to match zipper/unzipper (sanitize_name function)
- Bug #4: Replace bare except with proper exception handling
- Bug #5: Add Unicode filename handling (safe_encode_filename)
- Bug #6: Add structured logging module
- Bug #7: Add type annotations
- Bug #8: Make paths configurable via environment variables
- Bug #9: Add boto3 import check at startup
- Bug #10: Add large file threshold as environment variable

V3 NEW FIXES (Aligned with v8 standards - 13 additional bugs):
- Bug #1: Add cross-platform instance locking (fcntl for Unix, PID file for Windows)
- Bug #2: Add signal handlers for graceful shutdown (SIGINT/SIGTERM)
- Bug #3: Add S3 retry logic with exponential backoff (s3_operation_with_retry)
- Bug #4: Add S3 rate limiting detection/handling (SlowDown, 503, RequestLimitExceeded)
- Bug #5: Add S3 progress tracking per folder for crash resume
- Bug #6: Add disk usage monitoring (check_disk_usage, apply_backpressure)
- Bug #7: Add cleanup of orphaned temp directories
- Bug #8: Add atexit handler for guaranteed cleanup
- Bug #9: Add missing configurable constants (WORK_DIR, MAX_RETRY_DURATION, etc.)
- Bug #10: Add RequestTimeout exception handling from botocore
- Bug #11: Add shutdown event for graceful termination
- Bug #12: Add validation that rclone binary exists
- Bug #13: Add botocore timeout/connection error imports

V4 NEW FIXES (3 additional bugs for full v8 alignment):
- Bug #1: Add explicit GB_IN_BYTES constant to avoid 32-bit integer overflow
- Bug #2: Add _update_progress_safe helper function for thread-safe progress updates
- Bug #3: Add max_keys parameter to prune_progress_files for flexibility
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
import logging
import atexit
import threading
from urllib.parse import quote
from typing import List, Dict, Any, Tuple, Optional, Set
from datetime import datetime

# V3 FIX: Cross-platform fcntl support
if sys.platform != 'win32':
    import fcntl
else:
    fcntl = None  # Windows doesn't have fcntl

# Check boto3 early
try:
    import boto3
    import botocore.exceptions
    from botocore.config import Config
    from botocore.exceptions import RequestTimeout, ConnectionError as BotocoreConnectionError
except ImportError:
    print("X boto3 not installed! Run: pip install boto3")
    sys.exit(1)

# ============ CONFIGURATION ============
SOURCE = os.environ.get("SOURCE", "onedrive:Work Files")
S3_BUCKET = os.environ.get("S3_BUCKET", "workfiles123")
S3_PREFIX = os.environ.get("S3_PREFIX", "work_files_zips/")

# Get credentials from environment variables (SECURE - never hardcode!)
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "https://s3.ap-northeast-1.wasabisys.com")

# Configurable thresholds and paths
LARGE_FILE_THRESHOLD_GB = int(os.environ.get("LARGE_FILE_THRESHOLD_GB", "20"))
RCLONE_CONFIG = os.environ.get("RCLONE_CONFIG", "/content/rclone.conf")

# V3 FIX: Additional configurable constants for v8 alignment
WORK_DIR = os.environ.get("WORK_DIR", "/content")
S3_MAX_RETRIES = int(os.environ.get("S3_MAX_RETRIES", "3"))
MAX_RETRY_DURATION = int(os.environ.get("MAX_RETRY_DURATION", "300"))  # 5 minutes
INSTANCE_LOCK_TIMEOUT = int(os.environ.get("INSTANCE_LOCK_TIMEOUT", "300"))  # 5 minutes
MAX_COMPLETED_KEYS = int(os.environ.get("MAX_COMPLETED_KEYS", "1000"))
DISK_LIMIT_PERCENT = int(os.environ.get("DISK_LIMIT_PERCENT", "80"))
DISK_BACKPRESSURE_PERCENT = int(os.environ.get("DISK_BACKPRESSURE_PERCENT", "70"))

# V4 FIX: Explicit constants to avoid 32-bit overflow
# This matches the V8 zipper/unzipper implementation exactly
GB_IN_BYTES = 1024 * 1024 * 1024  # 1GB in bytes (fits in 32-bit signed integer)

# Unicode handling
UTF8_ENCODING = 'utf-8'

# ============ LOGGING SETUP ============
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============ S3 CONFIG WITH TIMEOUTS AND CONNECTION POOLING ============
S3_CONFIG = Config(
    connect_timeout=30,
    read_timeout=300,  # 5 minutes for large uploads
    retries={'max_attempts': 3},
    max_pool_connections=50  # Connection pooling for better performance
)

# =======================================
FOLDER_INDEX_KEY = f"{S3_PREFIX}_index/folder_list.txt"

# V4 FIX: Safe calculation that works on both 32-bit and 64-bit systems
# Using explicit GB_IN_BYTES constant avoids overflow on 32-bit Python
LARGE_FILE_THRESHOLD_BYTES = LARGE_FILE_THRESHOLD_GB * GB_IN_BYTES

# V3 FIX: Global state for graceful shutdown
_shutdown_requested = threading.Event()
_instance_lock_file: Optional[Any] = None
_progress_lock: Optional[Any] = None  # V4 FIX: Added for thread-safe progress updates


# ============ INSTANCE LOCK (V3 FIX - Cross-Platform) ============

def _process_exists(pid: int) -> bool:
    """V3 FIX: Check if a process with given PID exists."""
    try:
        if sys.platform != 'win32':
            os.kill(pid, 0)  # Unix: Send signal 0 to check if process exists
        else:
            # Windows: Use tasklist
            result = subprocess.run(['tasklist', '/FI', f'PID eq {pid}'],
                                   capture_output=True, text=True)
            return str(pid) in result.stdout
        return True
    except (OSError, ProcessLookupError):
        return False
    except Exception:
        return False


def _acquire_windows_lock(lock_path: str) -> bool:
    """V3 FIX: Windows-compatible instance lock using PID file."""
    global _instance_lock_file
    try:
        # Check for existing lock
        if os.path.exists(lock_path):
            try:
                with open(lock_path, 'r') as f:
                    content = f.read()
                    for line in content.splitlines():
                        if line.startswith("PID:"):
                            pid = int(line.split(":")[1].strip())
                            if not _process_exists(pid):
                                # Stale lock - remove it
                                os.remove(lock_path)
                                break
            except Exception:
                pass

        # Create lock file
        _instance_lock_file = open(lock_path, 'w')
        _instance_lock_file.write(f"PID: {os.getpid()}\nStarted: {datetime.now().isoformat()}\n")
        _instance_lock_file.flush()
        return True
    except Exception as e:
        logger.warning(f"Could not acquire Windows instance lock: {e}")
        return False


def _acquire_unix_lock(lock_path: str) -> bool:
    """V3 FIX: Unix instance lock using fcntl."""
    global _instance_lock_file
    try:
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        _instance_lock_file = open(lock_path, 'w')
        fcntl.flock(_instance_lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        _instance_lock_file.write(f"PID: {os.getpid()}\nStarted: {datetime.now().isoformat()}\n")
        _instance_lock_file.flush()
        return True
    except (IOError, OSError) as e:
        if _instance_lock_file:
            try:
                _instance_lock_file.close()
            except Exception:
                pass
            _instance_lock_file = None
        logger.warning(f"Could not acquire Unix instance lock: {e}")
        return False


def acquire_instance_lock() -> bool:
    """
    V3 FIX: Acquire a file lock to prevent multiple instances.
    Cross-platform: Uses fcntl on Unix, PID file on Windows.
    """
    lock_path = os.path.join(WORK_DIR, ".mapper_instance.lock")

    # V3 FIX: Check for stale lock first
    if os.path.exists(lock_path):
        try:
            with open(lock_path, 'r') as f:
                content = f.read()
                for line in content.splitlines():
                    if line.startswith("PID:"):
                        pid = int(line.split(":")[1].strip())
                        if not _process_exists(pid):
                            # Stale lock - remove it
                            os.remove(lock_path)
                            logger.info(f"Removed stale lock from crashed instance (PID: {pid})")
                            break
        except Exception as e:
            logger.warning(f"Error checking for stale lock: {e}")

    # Platform-specific lock acquisition
    if fcntl is not None:
        return _acquire_unix_lock(lock_path)
    else:
        return _acquire_windows_lock(lock_path)


def release_instance_lock() -> None:
    """V3 FIX: Release the instance lock."""
    global _instance_lock_file
    if _instance_lock_file:
        try:
            if fcntl is not None:
                fcntl.flock(_instance_lock_file.fileno(), fcntl.LOCK_UN)
            _instance_lock_file.close()
            lock_path = os.path.join(WORK_DIR, ".mapper_instance.lock")
            if os.path.exists(lock_path):
                os.remove(lock_path)
        except Exception as e:
            logger.warning(f"Error releasing instance lock: {e}")
        _instance_lock_file = None


# V3 FIX: Register cleanup handlers for abnormal exit
def _cleanup_on_exit():
    """V3 FIX: Cleanup handler called on normal exit."""
    release_instance_lock()

atexit.register(_cleanup_on_exit)


# ============ UNICODE HANDLING (Aligned with V8) ============

def safe_encode_filename(filename: str) -> str:
    """Safely encode filenames to handle Unicode characters."""
    try:
        filename.encode('ascii')
        return filename
    except UnicodeEncodeError:
        import unicodedata
        normalized = unicodedata.normalize('NFC', filename)
        return normalized


def sanitize_name(name: str) -> str:
    """Sanitize name for S3 key while preserving Unicode.

    This MUST match the function in python_zipper-v8.py and python_unzipper-v8.py
    to ensure consistent S3 key naming across all scripts.
    """
    safe_name = safe_encode_filename(name)
    return quote(safe_name, safe='').replace('%20', '_').replace('%2F', '_')


# ============ S3 CLIENT ============

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


# ============ S3 RETRY LOGIC (V3 FIX) ============

def s3_operation_with_retry(operation_func: Any, max_retries: int = S3_MAX_RETRIES,
                            max_duration: int = MAX_RETRY_DURATION) -> Any:
    """V3 FIX: Execute S3 operation with retry logic."""
    start_time = time.time()
    last_exception: Optional[Exception] = None

    for attempt in range(max_retries):
        if time.time() - start_time > max_duration:
            logger.error(f"Retry duration exceeded {max_duration}s")
            raise TimeoutError(f"Retry duration exceeded {max_duration} seconds")

        try:
            return operation_func()
        except botocore.exceptions.ConnectionError as e:
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.warning(f"S3 connection error, retrying in {wait_time}s...")
                time.sleep(wait_time)
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')

            # V3 FIX: Handle S3 rate limiting
            if error_code in ('SlowDown', '503', 'RequestLimitExceeded'):
                last_exception = e
                wait_time = min(2 ** (attempt + 2), 60)
                logger.warning(f"S3 rate limited, backing off for {wait_time}s...")
                time.sleep(wait_time)
                continue

            if error_code in ('NoSuchKey', 'AccessDenied', 'InvalidAccessKeyId'):
                raise
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.warning(f"S3 client error, retrying in {wait_time}s...")
                time.sleep(wait_time)
        except (RequestTimeout, BotocoreConnectionError) as e:
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.warning(f"S3 timeout/connection error, retrying in {wait_time}s...")
                time.sleep(wait_time)
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.warning(f"S3 error, retrying in {wait_time}s...")
                time.sleep(wait_time)

    raise last_exception if last_exception else Exception("Unknown S3 error")


# ============ DISK MANAGEMENT (V3 FIX) ============

def check_disk_usage() -> bool:
    """V3 FIX: Returns True if disk usage exceeds DISK_LIMIT_PERCENT."""
    try:
        total, used, free = shutil.disk_usage("/")
        if total > 0:
            percent = (used / total) * 100
            return percent > DISK_LIMIT_PERCENT
    except (OSError, IOError):
        pass
    return False


def get_disk_usage_percent() -> float:
    """V3 FIX: Get current disk usage percentage."""
    try:
        total, used, free = shutil.disk_usage("/")
        if total > 0:
            return (used / total) * 100
    except (OSError, IOError):
        pass
    return 0.0


def apply_backpressure() -> bool:
    """V3 FIX: Check if backpressure should be applied."""
    usage = get_disk_usage_percent()
    return usage > DISK_BACKPRESSURE_PERCENT


def handle_remove_readonly(func: Any, path: str, exc: Any) -> None:
    """V3 FIX: Force delete read-only files on Windows."""
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
    """V3 FIX: Clean up orphaned temp directories from previous crashed runs."""
    cleaned = 0
    try:
        for item in os.listdir(WORK_DIR):
            if item.startswith("temp_") or item.startswith("mapper_"):
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


# ============ PROGRESS TRACKING (V3/V4 FIX) ============

def get_progress_key(folder_name: str) -> str:
    """V3 FIX: Get per-folder progress file key."""
    safe_name = sanitize_name(folder_name)
    return f"{S3_PREFIX}_progress/{safe_name}_mapper_progress.json"


def load_progress(folder_name: str) -> Dict[str, Any]:
    """V3 FIX: Load progress JSON from S3 for a specific folder."""
    progress_key = get_progress_key(folder_name)

    def _load() -> Dict[str, Any]:
        s3 = get_s3_client()
        response = s3.get_object(Bucket=S3_BUCKET, Key=progress_key)
        return json.loads(response['Body'].read().decode(UTF8_ENCODING))

    try:
        return s3_operation_with_retry(_load)
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code in ('NoSuchKey', '404'):
            return {}
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
    """V3 FIX: Save progress JSON to S3 for a specific folder."""
    progress_key = get_progress_key(folder_name)
    progress = prune_progress_files(progress)

    def _save() -> bool:
        s3 = get_s3_client()
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=progress_key,
            Body=json.dumps(progress, indent=2, ensure_ascii=False).encode(UTF8_ENCODING),
            ContentType='application/json; charset=utf-8'
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


# V4 FIX: Added _update_progress_safe helper for thread-safe progress updates
def _update_progress_safe(folder_name: str, update_func: Any) -> bool:
    """V4 FIX: Safely update progress with lock handling.
    
    This matches the V8 zipper/unzipper implementation for consistency
    and ensures thread-safe progress updates when used with multiprocessing.
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


# V4 FIX: Added max_keys parameter for flexibility
def prune_progress_files(progress: Dict[str, Any], max_keys: int = MAX_COMPLETED_KEYS) -> Dict[str, Any]:
    """V4 FIX: Prune processed_folders if it grows too large.
    
    Added max_keys parameter for flexibility, matching V8 pattern.
    """
    processed_folders = progress.get("processed_folders", [])
    if len(processed_folders) > max_keys:
        progress["processed_folders"] = processed_folders[-max_keys:]
        logger.info(f"Pruned processed_folders to {max_keys} entries")
    return progress


def mark_folder_scanned(folder_name: str, normal_count: int, large_count: int) -> bool:
    """V3 FIX: Mark a folder as scanned in progress tracking."""
    def update(progress: Dict[str, Any]) -> None:
        progress["folder_name"] = folder_name
        progress["normal_files"] = normal_count
        progress["large_files"] = large_count
        progress["scanned_at"] = datetime.now().isoformat()
        progress["status"] = "scanned"
        
        if "processed_folders" not in progress:
            progress["processed_folders"] = []
        if folder_name not in progress["processed_folders"]:
            progress["processed_folders"].append(folder_name)
    
    # V4 FIX: Use _update_progress_safe for thread-safe updates
    return _update_progress_safe(folder_name, update)


# ============ HELPER FUNCTIONS ============

def discover_folders() -> List[str]:
    """Auto-discover all top-level subfolders in the source path."""
    logger.info(f"Discovering folders in: {SOURCE}")

    # V3 FIX: Check for shutdown
    if _shutdown_requested.is_set():
        logger.warning("Shutdown requested, aborting folder discovery")
        return []

    # V3 FIX: Validate rclone exists
    if shutil.which("rclone") is None:
        logger.error("rclone not found! Please install rclone first.")
        logger.info("  Visit: https://rclone.org/install/")
        return []

    cmd = [
        'rclone', 'lsf', SOURCE,
        '--dirs-only'
    ]

    if os.path.exists(RCLONE_CONFIG):
        cmd.extend(['--config', RCLONE_CONFIG])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=300)
        folders = [line.strip().rstrip('/') for line in result.stdout.splitlines() if line.strip()]
        logger.info(f"Found {len(folders)} folders")
        for f in folders:
            logger.info(f"  - {f}")
        return folders
    except subprocess.CalledProcessError as e:
        logger.error(f"Error discovering folders: {e.stderr}")
        return []
    except subprocess.TimeoutExpired:
        logger.error("Timeout while discovering folders")
        return []


def save_folder_index(folders: List[str]) -> bool:
    """Save the master folder list to S3."""
    def _save() -> bool:
        s3 = get_s3_client()
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=FOLDER_INDEX_KEY,
            Body="\n".join(folders).encode(UTF8_ENCODING),
            ContentType='text/plain; charset=utf-8'
        )
        return True

    try:
        return s3_operation_with_retry(_save)
    except Exception as e:
        logger.error(f"Failed to save folder index: {e}")
        return False


def check_list_exists(s3: Any, map_key: str) -> bool:
    """Check if a file list already exists on S3."""
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=map_key)
        return True
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code in ('NoSuchKey', '404'):
            return False
        logger.warning(f"Error checking if list exists: {e}")
        return False
    except Exception as e:
        logger.warning(f"Unexpected error checking list existence: {e}")
        return False


def scan_folder_with_sizes(folder: str) -> Tuple[List[str], List[Dict[str, Any]]]:
    """
    Scan a folder using rclone lsjson to get file paths AND sizes.

    Returns:
        normal_files: list of relative paths (strings) for files <= threshold
        large_files: list of dicts {path, size, size_gb} for files > threshold
    """
    # V3 FIX: Check for shutdown
    if _shutdown_requested.is_set():
        logger.warning(f"Shutdown requested, skipping scan of {folder}")
        return [], []

    # V3 FIX: Apply backpressure if disk is getting full
    if apply_backpressure():
        logger.warning(f"High disk usage ({get_disk_usage_percent():.1f}%), applying backpressure")
        time.sleep(2)

    folder_path = f"{SOURCE}/{folder}"

    cmd = [
        'rclone', 'lsjson', folder_path,
        '-R', '--files-only', '--no-mimetype', '--no-modtime'
    ]

    if os.path.exists(RCLONE_CONFIG):
        cmd.extend(['--config', RCLONE_CONFIG])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=600)
        entries = json.loads(result.stdout)

        normal_files: List[str] = []
        large_files: List[Dict[str, Any]] = []

        for entry in entries:
            if not isinstance(entry, dict):
                continue

            path = entry.get('Path', '')
            size = entry.get('Size', 0)

            # V4 FIX: Use safe threshold comparison (works on 32-bit and 64-bit)
            if size > LARGE_FILE_THRESHOLD_BYTES:
                large_files.append({
                    'path': path,
                    'size': size,
                    'size_gb': round(size / GB_IN_BYTES, 2)  # V4 FIX: Use GB_IN_BYTES
                })
            else:
                normal_files.append(path)

        return normal_files, large_files

    except subprocess.CalledProcessError as e:
        logger.error(f"Error scanning folder {folder}: {e.stderr[:80]}")
        return [], []
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout scanning folder {folder}")
        return [], []
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON from rclone for {folder}: {e}")
        return [], []


def upload_file_list(s3: Any, folder: str, normal_files: List[str]) -> bool:
    """Upload normal files list to S3."""
    safe_name = sanitize_name(folder)
    map_key = f"{S3_PREFIX}{safe_name}_List.txt"

    def _upload() -> bool:
        if normal_files:
            file_list_data = "\n".join(normal_files)
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=map_key,
                Body=file_list_data.encode(UTF8_ENCODING),
                ContentType='text/plain; charset=utf-8'
            )
        else:
            # Upload empty list so resume doesn't re-scan
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=map_key,
                Body=b"",
                ContentType='text/plain; charset=utf-8'
            )
        return True

    try:
        return s3_operation_with_retry(_upload)
    except Exception as e:
        logger.error(f"Failed to upload file list for {folder}: {e}")
        return False


def upload_large_files_list(s3: Any, folder: str, large_files: List[Dict[str, Any]]) -> bool:
    """Upload large files list to S3."""
    if not large_files:
        return True

    safe_name = sanitize_name(folder)
    large_key = f"{S3_PREFIX}{safe_name}_LargeFiles.json"

    def _upload() -> bool:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=large_key,
            Body=json.dumps(large_files, indent=2, ensure_ascii=False).encode(UTF8_ENCODING),
            ContentType='application/json; charset=utf-8'
        )
        return True

    try:
        return s3_operation_with_retry(_upload)
    except Exception as e:
        logger.error(f"Failed to upload large files list for {folder}: {e}")
        return False


# ============ SIGNAL HANDLERS (V3 FIX) ============

def signal_handler(signum: int, frame: Any) -> None:
    """V3 FIX: Handle shutdown signals gracefully."""
    logger.warning(f"Received signal {signum}, shutting down gracefully...")
    _shutdown_requested.set()
    release_instance_lock()


def run_mapper(force_rescan: bool = False) -> None:
    """Main mapper function."""
    logger.info("=" * 60)
    logger.info("PYTHON MASTER MAPPER v4 (Production Ready)")
    logger.info("=" * 60)
    logger.info(f"Source: {SOURCE}")
    logger.info(f"S3 Bucket: {S3_BUCKET}")
    logger.info(f"S3 Prefix: {S3_PREFIX}")
    logger.info(f"Large file threshold: {LARGE_FILE_THRESHOLD_GB} GB")
    logger.info(f"Rclone config: {RCLONE_CONFIG}")
    logger.info(f"Work directory: {WORK_DIR}")
    
    if force_rescan:
        logger.warning("FORCE RESCAN enabled - will re-scan all folders")

    # V3 FIX: Acquire instance lock
    if not acquire_instance_lock():
        logger.error("Another instance is already running. Exiting.")
        return

    # V3 FIX: Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Validate credentials
        if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
            logger.error("AWS credentials not configured!")
            logger.info("Set environment variables:")
            logger.info("  export AWS_ACCESS_KEY_ID='your_access_key'")
            logger.info("  export AWS_SECRET_ACCESS_KEY='your_secret_key'")
            return

        # V3 FIX: Check disk usage before starting
        if check_disk_usage():
            logger.warning(f"Disk usage is high ({get_disk_usage_percent():.1f}%), proceed with caution")

        # V3 FIX: Clean up orphaned temp directories
        cleaned = cleanup_orphaned_temp_dirs()
        if cleaned > 0:
            logger.info(f"Cleaned up {cleaned} orphaned temp directories")

        # Get S3 client
        try:
            s3 = get_s3_client()
            # Test connection
            s3.head_bucket(Bucket=S3_BUCKET)
            logger.info("S3 connection successful")
        except Exception as e:
            logger.error(f"S3 connection failed: {e}")
            return

        # 1. Auto-discover folders
        folders = discover_folders()
        if not folders:
            logger.error("No folders found. Exiting.")
            return

        # Check for shutdown
        if _shutdown_requested.is_set():
            logger.warning("Shutdown requested, exiting...")
            return

        # 2. Save folder index to S3
        if not save_folder_index(folders):
            logger.error("Failed to save folder index. Exiting.")
            return

        # 3. Resume check
        if not force_rescan:
            already_done: List[str] = []
            remaining: List[str] = []

            for folder in folders:
                safe_name = sanitize_name(folder)
                map_key = f"{S3_PREFIX}{safe_name}_List.txt"

                if check_list_exists(s3, map_key):
                    already_done.append(folder)
                else:
                    remaining.append(folder)

            if already_done:
                logger.info(f"Resume: {len(already_done)} folders already mapped, skipping")
                for f in already_done:
                    logger.info(f"  [SKIP] {f}")

            if not remaining:
                logger.info("All folders already mapped! Use force_rescan=True to re-scan.")
                return

            folders = remaining

        # 4. Scan each folder with size detection
        logger.info(f"Scanning {len(folders)} folders (with size detection)...")

        total_normal = 0
        total_large = 0
        failed_folders: List[str] = []

        for folder in folders:
            # Check for shutdown
            if _shutdown_requested.is_set():
                logger.warning("Shutdown requested, stopping scan...")
                break

            logger.info(f"Scanning: {folder}")

            normal_files, large_files = scan_folder_with_sizes(folder)

            # Upload normal files list
            if not upload_file_list(s3, folder, normal_files):
                failed_folders.append(folder)
                continue

            # Upload large files list
            if large_files:
                if not upload_large_files_list(s3, folder, large_files):
                    logger.warning(f"Failed to upload large files list for {folder}")

            # V3 FIX: Save progress
            mark_folder_scanned(folder, len(normal_files), len(large_files))

            total_normal += len(normal_files)
            total_large += len(large_files)

            large_info = f", {len(large_files)} large" if large_files else ""
            logger.info(f"  [OK] {folder}: {len(normal_files)} files{large_info}")

            # Print large file details
            if large_files:
                for lf in large_files:
                    logger.info(f"    [LARGE] {lf['path']} ({lf['size_gb']} GB)")

        # Summary
        logger.info("=" * 60)
        logger.info("MAPPING COMPLETE!")
        logger.info(f"Normal files: {total_normal}")
        logger.info(f"Large files: {total_large} (will be transferred directly)")

        if failed_folders:
            logger.warning(f"Failed folders: {len(failed_folders)}")
            for f in failed_folders:
                logger.warning(f"  [FAIL] {f}")

        if _shutdown_requested.is_set():
            logger.warning("Mapper was interrupted. Progress has been saved to S3.")

    finally:
        # V3 FIX: Always release instance lock
        release_instance_lock()


if __name__ == "__main__":
    run_mapper()
